import os
import re
import yaml
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass

@dataclass
class Column:
    name: str
    data_type: str = 'unknown'
    description: str = ''
    tests: List[str] = None
    references: Dict[str, str] = None

@dataclass
class Model:
    name: str
    type: str  # 'fact', 'dimension', or 'staging'
    sql_path: str
    yaml_path: str
    columns: List[Column] = None
    description: str = ''
    tests: List[str] = None
    references: Set[str] = None

class ModelAnalyzer:
    def __init__(self, project_root: str):
        self.project_root = project_root
        self.models_dir = os.path.join(project_root, 'models')
        self.models: Dict[str, Model] = {}
        self.load_models()
    def load_models(self):
        """Load all models from the dbt project"""
        for root, _, files in os.walk(self.models_dir):
            for file in files:
                if file.endswith('.sql'):
                    model_path = os.path.join(root, file)
                    model_name = os.path.splitext(file)[0]
                    yaml_path = os.path.join(root, 'schema.yml')
                    
                    if not os.path.exists(yaml_path):
                        yaml_path = None
                    
                    model_type = self._determine_model_type(model_name)
                    
                    self.models[model_name] = Model(
                        name=model_name,
                        type=model_type,
                        sql_path=model_path,
                        yaml_path=yaml_path
                    )

    def _determine_model_type(self, model_name: str) -> str:
        """Determine if model is fact, dimension, or staging"""
        if model_name.startswith('fct_'):
            return 'fact'
        elif model_name.startswith('dim_'):
            return 'dimension'
        elif model_name.startswith('stg_'):
            return 'staging'
        return 'other'
    
    def _extract_refs(self, sql_content: str) -> Set[str]:
        """Extract model references from SQL using regex"""
        ref_pattern = r'{{\s*ref\([\'"]([\w]+)[\'"]\)\s*}}'
        return set(re.findall(ref_pattern, sql_content))

    def _extract_columns(self, sql_content: str) -> List[str]:
        """Extract column names from SQL using regex"""
        # Find the final SELECT statement
        final_select = sql_content.split('final as (')[-1] if 'final as (' in sql_content else sql_content
        
        # Extract column names
        col_pattern = r'(?:select|,)\s+(?:.*?\s+as\s+)?([a-zA-Z_][a-zA-Z0-9_]*)(?=\s*(?:,|from|$))'
        return re.findall(col_pattern, final_select, re.IGNORECASE)
    
    def analyze_model(self, model_name: str) -> Dict:
        """Analyze a single model"""
        model = self.models.get(model_name)
        if not model:
            return {"error": f"Model {model_name} not found"}

        # Read SQL content
        with open(model.sql_path, 'r') as f:
            sql_content = f.read()

        # Extract references and columns
        model.references = self._extract_refs(sql_content)
        model.columns = [Column(name=col) for col in self._extract_columns(sql_content)]

        # Read YAML content if available
        if model.yaml_path:
            with open(model.yaml_path, 'r') as f:
                yaml_content = yaml.safe_load(f)
                for yaml_model in yaml_content.get('models', []):
                    if yaml_model['name'] == model_name:
                        model.description = yaml_model.get('description', '')
                        model.tests = yaml_model.get('tests', [])
                        
                        # Update column metadata
                        yaml_columns = {col['name']: col for col in yaml_model.get('columns', [])}
                        for col in model.columns:
                            if col.name in yaml_columns:
                                yaml_col = yaml_columns[col.name]
                                col.description = yaml_col.get('description', '')
                                col.tests = yaml_col.get('tests', [])

        # Analyze model structure
        analysis = {
            "name": model.name,
            "type": model.type,
            "column_count": len(model.columns),
            "reference_count": len(model.references),
            "references": list(model.references),
            "columns": {},
            "kimball_compliance": self._check_kimball_compliance(model)
        }

        # Categorize columns
        key_cols = []
        measure_cols = []
        timestamp_cols = []
        attribute_cols = []

        for col in model.columns:
            if col.name.endswith('_key') or col.name.endswith('_id'):
                key_cols.append(col.name)
            elif col.name in ['amount', 'quantity', 'weight', 'price', 'cost']:
                measure_cols.append(col.name)
            elif col.name in ['created_at', 'updated_at', 'order_date']:
                timestamp_cols.append(col.name)
            else:
                attribute_cols.append(col.name)

            analysis["columns"][col.name] = {
                "description": col.description,
                "tests": col.tests,
                "category": "key" if col.name in key_cols else
                           "measure" if col.name in measure_cols else
                           "timestamp" if col.name in timestamp_cols else
                           "attribute"
            }

        analysis["structure"] = {
            "key_columns": key_cols,
            "measure_columns": measure_cols,
            "timestamp_columns": timestamp_cols,
            "attribute_columns": attribute_cols
        }

        return analysis

    def _check_kimball_compliance(self, model: Model) -> Dict:
        """Check if model follows Kimball dimensional modeling principles"""
        compliance = {
            "status": "compliant",
            "warnings": []
        }

        if model.type == 'dimension':
            # Check dimension table compliance
            has_surrogate_key = any(col.name.endswith('_key') for col in model.columns)
            has_natural_key = any(col.name.endswith('_id') for col in model.columns)
            has_timestamps = any(col.name in ['created_at', 'updated_at'] for col in model.columns)

            if not has_surrogate_key:
                compliance["warnings"].append("Missing surrogate key")
            if not has_natural_key:
                compliance["warnings"].append("Consider adding natural key")
            if not has_timestamps:
                compliance["warnings"].append("Missing audit timestamps")

        elif model.type == 'fact':
            # Check fact table compliance
            has_measures = any(col.name in ['amount', 'quantity', 'weight', 'price', 'cost'] 
                             for col in model.columns)
            has_foreign_keys = any(col.name.endswith('_key') and col.name != f"{model.name}_key" 
                                 for col in model.columns)

            if not has_measures:
                compliance["warnings"].append("No measures found in fact table")
            if not has_foreign_keys:
                compliance["warnings"].append("No foreign keys to dimensions")

        if compliance["warnings"]:
            compliance["status"] = "warnings"

        return compliance

    def print_analysis(self, analysis: Dict):
        """Print analysis results in a readable format"""
        print(f"\n=== Model Analysis: {analysis['name']} ===")
        print(f"Type: {analysis['type']}")
        print(f"Column Count: {analysis['column_count']}")
        print(f"References: {', '.join(analysis['references'])}")

        print("\nStructure:")
        for category, cols in analysis['structure'].items():
            if cols:
                print(f"  {category}: {', '.join(cols)}")

        print("\nKimball Compliance:")
        print(f"  Status: {analysis['kimball_compliance']['status']}")
        if analysis['kimball_compliance']['warnings']:
            print("  Warnings:")
            for warning in analysis['kimball_compliance']['warnings']:
                print(f"    - {warning}")

        print("\nColumns:")
        for col_name, col_info in analysis['columns'].items():
            print(f"  {col_name}:")
            print(f"    Category: {col_info['category']}")
            if col_info['description']:
                print(f"    Description: {col_info['description']}")
            if col_info.get('tests'):
                tests = []
                for t in col_info['tests']:
                    if isinstance(t, str):
                        tests.append(t)
                    elif isinstance(t, dict) and 'name' in t:
                        tests.append(t['name'])
                if tests:
                    print(f"    Tests: {', '.join(tests)}")

def main():
    analyzer = ModelAnalyzer(os.path.join(os.getcwd()))
    
    models_to_analyze = [
        'fct_sales',
        'dim_products',
        'dim_dates',
        'dim_geography',
        'stg_sales'
    ]
    
    for model_name in models_to_analyze:
        analysis = analyzer.analyze_model(model_name)
        analyzer.print_analysis(analysis)

if __name__ == '__main__':
    main()
