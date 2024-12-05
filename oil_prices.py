import pandas as pd
import requests
from bs4 import BeautifulSoup
import uuid
from datetime import datetime
import sys


def fetch_oil_prices(target_months):
    url = "https://vipmbr.cpc.com.tw/mbwebs/showhistoryprice_oil.aspx"
    response = requests.get(url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'lxml')

    table = soup.find('table', id='MyGridView')

    rows = table.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        if len(cols) > 0:
            data.append(cols)

    header = [th.text.strip() for th in table.find_all('th')]
    target_columns = ['調價日期', '無鉛汽油92', '無鉛汽油95', '無鉛汽油98', '超級/高級柴油']
    column_indices = [header.index(col) for col in target_columns]

    filtered_data = [[row[i] for i in column_indices] for row in data]

    df = pd.DataFrame(filtered_data, columns=target_columns)
    df.replace("", None, inplace=True)
    df['調價日期'] = pd.to_datetime(df['調價日期'], errors='coerce')

    for col in ['無鉛汽油92', '無鉛汽油95', '無鉛汽油98', '超級/高級柴油']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if target_months:
        df = df[df['調價日期'].dt.strftime('%Y/%m').isin(target_months)]

    return df


def calculate_monthly_average(df):
    df['月份'] = df['調價日期'].dt.to_period('M')

    monthly_avg = (
        df.groupby('月份')[['無鉛汽油92', '無鉛汽油95', '無鉛汽油98', '超級/高級柴油']]
        .apply(lambda x: x.mean(skipna=True))
        .round(2)
        .reset_index()
    )

    monthly_avg['月份'] = monthly_avg['月份'].astype(str)

    return monthly_avg


def generate_sql_from_dataframe(df):
    sql_statements = []
    oil_types = {
        '無鉛汽油92': '92無鉛汽油',
        '無鉛汽油95': '95無鉛汽油',
        '無鉛汽油98': '98無鉛汽油',
        '超級/高級柴油': '超級柴油'
    }
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for _, row in df.iterrows():
        year, month = row['月份'].split('-')
        for oil_column, oil_type in oil_types.items():
            if pd.notna(row[oil_column]):
                sql = f"""
                INSERT INTO otdb064_oil_prices
                (id, oil_year, oil_month, oil_type, oil_unit, oil_price, delete_flag, options_system, options_user, created_at, updated_at, created_by, updated_by)
                VALUES('{uuid.uuid4()}', {year}, {int(month)}, '{oil_type}', '公升', {row[oil_column]}, 0, '', '', '{current_time}', '{current_time}', 'admin', 'admin');
                """
                sql_statements.append(sql.strip())
    return sql_statements


def main():
    # 從命令列獲取參數
    if len(sys.argv) < 2:
        print("請提供 target_months 參數，例如：2024/10,2024/11,2024/12")
        sys.exit(1)

    # 解析 target_months 參數
    target_months = sys.argv[1].split(',')

    # 執行流程
    result = fetch_oil_prices(target_months=target_months)
    monthly_avg_result = calculate_monthly_average(result)
    sql_results = generate_sql_from_dataframe(monthly_avg_result)

    # 產生檔案名稱
    today_date = datetime.now().strftime('%Y-%m-%d')
    calculated_months = "_".join(target_months).replace("/", "-")
    file_name = f"oil_prices_{today_date}_{calculated_months}.sql"

    # 寫入 SQL 檔案
    with open(file_name, "w", encoding="utf-8") as file:
        file.write("\n".join(sql_results))

    print(f"SQL 檔案已產生：{file_name}")


if __name__ == "__main__":
    main()
