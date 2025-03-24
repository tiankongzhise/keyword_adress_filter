from tkzs_bd_db_tool import get_session,init_db
from tkzs_bd_db_tool import models

import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_local_result(file_dir):
    result = []
    for filename in os.listdir(file_dir):
        if not filename.endswith(('.xlsx', '.xls')) and not filename.startwith('result_'):
            continue
        file_path = os.path.join(file_dir, filename)
        try:
            xl = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"无法读取文件 {file_path}: {str(e)}")
            continue
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                df.rename(columns={'关键词': 'keyword', '是否深圳地区': 'is_shenzhen',"省份":"province","地域":"region"}, inplace=True)
                result.extend(df.to_dict('records'))
            except Exception as e:
                print(f"处理工作表 {sheet_name} 时出错: {str(e)}")
                continue
    return result




def get_update_list(result:list,records:list)->tuple[list[dict],list[dict]]:
    result_mapping_dict = {}
    for item in result:
        result_mapping_dict[item['keyword']] = {
            'is_shenzhen': item['is_shenzhen'],
            'province': item['province'],
            'region': item['region']
        }
    print(result_mapping_dict)
    db_mapping_dict = {}
    for item in records:
        db_mapping_dict[item.keyword] = {
            'is_shenzhen': item.is_shenzhen,
            'province': item.province,
            'region': item.region,
            'id': item.id
        }
    print(db_mapping_dict)
    update_list = []
    insert_list = []
    for key,value in result_mapping_dict.items():

        if key not in db_mapping_dict.keys():
            insert_list.append({
                'keyword': key,
                'is_shenzhen': value['is_shenzhen'],
                'province': value['province'],
                'region': value['region']
            })
            continue

        if value['province'] == db_mapping_dict[key]['province'] and value['region'] == db_mapping_dict[key]['region']:
            if value['is_shenzhen'] == db_mapping_dict[key]['is_shenzhen']:
                continue
            else:
                update_list.append({
                    'id': db_mapping_dict[key]['id'],
                    'is_shenzhen': value['is_shenzhen'],
                })
        else:
            update_list.append({
                'id': db_mapping_dict[key]['id'],
                'is_shenzhen': value['is_shenzhen'],
                'province': value['province'],
                'region': value['region']
            })
    return update_list,insert_list








if __name__ == '__main__':
    file_dir = r'./data'
    result = get_local_result(file_dir)
    print(result)
    try:
        init_db()
        with get_session() as session:
            records = session.query(models.KeywordFilterAddress).all()
            update_list,insert_list = get_update_list(result,records)
            print('更新数据库')
            session.bulk_update_mappings(models.KeywordFilterAddress, update_list)
            if insert_list:
                session.bulk_insert_mappings(models.KeywordFilterAddress, insert_list)
            session.commit()
    except Exception as e:
        print(f"数据库操作出错: {str(e)}")
        session.rollback()
