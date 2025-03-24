import os
import pandas as pd

def get_all_keywords():
    keywords = set()
    keyword_dir = 'keyword_data'
    for filename in os.listdir(keyword_dir):
        if not filename.endswith(('.xlsx', '.xls')):
            continue
        file_path = os.path.join(keyword_dir, filename)
        try:
            xl = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"无法读取文件 {file_path}: {str(e)}")
            continue
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                if '关键词' in df.columns:
                    keywords.update(df['关键词'].dropna().astype(str))
            except Exception as e:
                print(f"处理工作表 {sheet_name} 时出错: {str(e)}")
                continue
    return iter(keywords)