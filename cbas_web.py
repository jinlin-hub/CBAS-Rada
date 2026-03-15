import streamlit as st
import pandas as pd
import numpy as np
import re
import urllib.request
import urllib.parse
import json
import ssl
from datetime import datetime, timedelta
import io

# 🎯 關閉安全警告與憑證檢查
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

def get_stock_price_yahoo(stock_id):
    """雅虎 API 直連"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    for suffix in [".TW", ".TWO"]:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?range=5d&interval=1d"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as response:
                data = json.loads(response.read().decode('utf-8'))
                result = data.get('chart', {}).get('result', [])
                if result:
                    price = result[0].get('meta', {}).get('regularMarketPrice')
                    if price and price > 0:
                        return round(price, 2)
        except: continue
    return 0

def get_cbas_info(cb_code):
    """從 thefew.tw 抓取 CBAS 權利金與轉換比例"""
    url = f"https://thefew.tw/quote/{cb_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as response:
            html = response.read().decode('utf-8', errors='ignore')
            clean_text = re.sub(r'<[^>]+>', ' ', html)
            
            cbas_match = re.search(r'CBAS\s*權利金[^\d]*([\d\.]+)', clean_text)
            cbas_premium = float(cbas_match.group(1)) if cbas_match else np.nan
            
            ratio_match = re.search(r'轉換比例[^\d]*([\d\.]+)', clean_text)
            ratio = float(ratio_match.group(1)) if ratio_match else np.nan
            
            return cbas_premium, ratio
    except Exception:
        return np.nan, np.nan

# ==========================================
# 🌐 Streamlit 網頁介面與核心邏輯
# ==========================================
st.set_page_config(page_title="CBAS 智慧雷達", layout="wide", page_icon="🚀")

st.title("🚀 CBAS 四雲端可轉債異常量能雷達")
st.markdown("點擊下方按鈕，系統將自動穿透防火牆，抓取最新量能、權利金與 **官方未平倉籌碼**。")

if st.button("⚡ 立即執行全自動掃描", type="primary"):
    
    with st.status("系統運算中，請稍候...", expanded=True) as status:
        
        # --- 引擎 A：轉換價格 ---
        st.write("🌐 (1/4) 正在獲取公開資訊觀測站「最新轉換價格表」...")
        conv_price_map = {}
        now = datetime.now()
        months_to_try = [now.strftime('%Y%m'), (now.replace(day=1) - timedelta(days=1)).strftime('%Y%m')]
        
        for yyyymm in months_to_try:
            base_url = f"https://mopsov.twse.com.tw/nas/t120/CBTRN{yyyymm}.htm"
            urls_to_try = [base_url, f"https://api.allorigins.win/raw?url={urllib.parse.quote(base_url)}"]
            
            for url in urls_to_try:
                try:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, context=ssl_ctx, timeout=15) as response:
                        html_content = response.read().decode('big5', errors='ignore')
                        rows = re.findall(r'<tr.*?>(.*?)</tr>', html_content, re.IGNORECASE | re.DOTALL)
                        for row in rows:
                            cols = re.findall(r'<td.*?>(.*?)</td>', row, re.IGNORECASE | re.DOTALL)
                            if len(cols) >= 5:
                                code_match = re.search(r'(\d{5,6})', cols[1])
                                if not code_match: continue
                                price_text = re.sub(r'<.*?>', '', cols[4]).replace(',', '').strip()
                                try:
                                    if float(price_text) > 0: conv_price_map[code_match.group(1)] = float(price_text)
                                except: continue
                except: continue
                if conv_price_map: break
            if conv_price_map: 
                st.write(f"✅ 成功載入 {len(conv_price_map)} 檔轉換價格")
                break

        if not conv_price_map:
            status.update(label="❌ 無法取得轉換價格", state="error")
            st.stop()

        # --- 引擎 D：櫃買中心 CBAS 未平倉量 (全新加入！) ---
        st.write("🌐 (2/4) 正在抓取櫃買中心「官方大戶未平倉籌碼」...")
        cbas_oi_map = {}
        # 往前推 7 天，找尋最新的一份未平倉報告
        for days_back in range(7):
            target_date = datetime.now() - timedelta(days=days_back)
            if target_date.weekday() >= 5: continue
            yyyy = target_date.strftime('%Y')
            yyyymm = target_date.strftime('%Y%m')
            yyyymmdd = target_date.strftime('%Y%m%d')
            
            oi_csv_base = f"https://www.tpex.org.tw/storage/bond_zone/tradeinfo/cbas/{yyyy}/{yyyymm}/RStc0111.{yyyymmdd}-C.csv"
            oi_urls = [oi_csv_base, f"https://api.allorigins.win/raw?url={urllib.parse.quote(oi_csv_base)}"]
            
            success = False
            for oi_url in oi_urls:
                try:
                    req = urllib.request.Request(oi_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, context=ssl_ctx, timeout=8) as response:
                        content = response.read().decode('cp950', errors='ignore')
                        for line in content.splitlines():
                            if line.startswith('BODY'):
                                # 清洗 CSV 欄位，過濾空白
                                cols = [c.strip().strip('"') for c in line.replace('\t', ',').split(',')]
                                cols = [c for c in cols if c]
                                if len(cols) >= 3:
                                    code_match = re.search(r'\d{5,6}', cols[0])
                                    if code_match:
                                        try:
                                            # 最後一個欄位即為未平倉餘額
                                            oi_val = float(cols[-1].replace(',', ''))
                                            cbas_oi_map[code_match.group(0)] = int(oi_val)
                                        except: pass
                        if cbas_oi_map:
                            st.write(f"✅ 成功載入 {len(cbas_oi_map)} 檔未平倉籌碼 ({yyyymmdd})")
                            success = True
                            break
                except: pass
            if success: break

        # --- 引擎 B：近期行情 ---
        st.write("🌐 (3/4) 正在下載近一個月行情資料 (計算量能)...")
        all_dfs = []
        valid_days = 0
        days_offset = 0

        while valid_days < 25 and days_offset < 60:
            target_date = datetime.now() - timedelta(days=days_offset)
            days_offset += 1
            if target_date.weekday() >= 5: continue
                
            yyyy = target_date.strftime('%Y')
            yyyymm = target_date.strftime('%Y%m')
            yyyymmdd = target_date.strftime('%Y%m%d')
            csv_base = f"https://www.tpex.org.tw/storage/bond_zone/tradeinfo/cb/{yyyy}/{yyyymm}/RSta0113.{yyyymmdd}-C.csv"
            csv_urls = [csv_base, f"https://api.allorigins.win/raw?url={urllib.parse.quote(csv_base)}"]
            
            success = False
            for csv_url in csv_urls:
                try:
                    req = urllib.request.Request(csv_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as response:
                        content = response.read().decode('cp950', errors='ignore')
                        lines = content.splitlines()
                        date_match = re.search(r'日期:(\d+)年(\d+)月(\d+)日', "".join(lines[:10]))
                        date_str = f"{int(date_match.group(1))+1911}-{date_match.group(2).zfill(2)}-{date_match.group(3).zfill(2)}" if date_match else "Unknown"
                        
                        rows = []
                        for line in lines:
                            if line.startswith('BODY'):
                                cols = [c.strip().strip('"') for c in line.replace('\t', ',').split(',')]
                                if '等價' in cols:
                                    idx = cols.index('等價')
                                    try:
                                        code_match = re.search(r'\d{5,6}', cols[0])
                                        code = code_match.group(0) if code_match else cols[idx - 2]
                                        rows.append([code, cols[idx - 1], cols[idx + 1], cols[idx + 7], date_str])
                                    except: continue
                        if rows:
                            all_dfs.append(pd.DataFrame(rows, columns=['代號', '名稱', '收市', '單位', 'Date']))
                            valid_days += 1
                            st.write(f"📥 成功下載 {yyyymmdd} 行情")
                            success = True
                            break
                except: pass
            if success: continue

        if len(all_dfs) < 20:
            status.update(label="❌ 有效天數不足，請稍後再試", state="error")
            st.stop()

        # --- 運算與整合 ---
        st.write("📈 (4/4) 正在執行 Z-Score 與全籌碼數據精算...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df['成交張數'] = pd.to_numeric(full_df['單位'].str.replace(',', ''), errors='coerce').fillna(0)
        full_df['CB收盤價'] = pd.to_numeric(full_df['收市'].str.replace(',', ''), errors='coerce').fillna(0)
        full_df = full_df.sort_values(['代號', 'Date'])

        grouped = full_df.groupby('代號')['成交張數']
        full_df['ma20'] = grouped.transform(lambda x: x.rolling(20).mean())
        full_df['std20'] = grouped.transform(lambda x: x.rolling(20).std())
        full_df['z_score'] = (full_df['成交張數'] - full_df['ma20']) / (full_df['std20'] + 0.1)

        latest = full_df.groupby('代號').tail(1).copy()
        latest['轉換價格'] = latest['代號'].map(conv_price_map).fillna(0)
        latest['現股代號'] = latest['代號'].str[:4]
        
        latest['等級'] = latest.apply(lambda r: '🔥 S級' if r['z_score'] > 2.33 and r['成交張數'] > 100 else ('⚠️ A級' if r['z_score'] > 1.645 and r['成交張數'] > 50 else 'Normal'), axis=1)
        results = latest[latest['等級'] != 'Normal'].copy()

        if not results.empty:
            results['現股收盤價'] = results['現股代號'].apply(get_stock_price_yahoo)
            
            # 從 TheFew 抓權利金與比例
            results['cbas_info'] = results['代號'].apply(get_cbas_info)
            results['CBAS權利金'] = results['cbas_info'].apply(lambda x: x[0])
            results['轉換比例'] = results['cbas_info'].apply(lambda x: x[1])
            
            # 從第 4 顆引擎 (官方籌碼) 對應未平倉量
            results['未平倉量(張)'] = results['代號'].map(cbas_oi_map).fillna(0)
            
            def calc_premium(row):
                if row['轉換價格'] > 0 and row['現股收盤價'] > 0:
                    conv_value = (100 / row['轉換價格']) * row['現股收盤價']
                    return round((row['CB收盤價'] / conv_value - 1) * 100, 2)
                return np.nan

            results['實際溢價率%'] = results.apply(calc_premium, axis=1)

            out_cols = ['Date', '代號', '名稱', '現股代號', '等級', '成交張數', 'z_score', 'CB收盤價', '轉換價格', '現股收盤價', '實際溢價率%', 'CBAS權利金', '轉換比例', '未平倉量(張)']
            final_df = results[out_cols]

            status.update(label="🎉 掃描完成！四引擎資料已整合", state="complete")
            
            st.subheader("📊 掃描結果預覽")
            st.dataframe(final_df, use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, index=False, sheet_name='異常爆量掃描')
                workbook  = writer.book
                worksheet = writer.sheets['異常爆量掃描']
                gold_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'font_color': '#B8860B', 'bold': True})
                grey_fmt = workbook.add_format({'bg_color': '#F2F2F2', 'font_color': '#A6A6A6'})
                worksheet.conditional_format(1, 10, len(final_df), 10, {'type': 'cell', 'criteria': '<=', 'value': 5, 'format': gold_fmt})
                worksheet.conditional_format(1, 10, len(final_df), 10, {'type': 'cell', 'criteria': '>', 'value': 30, 'format': grey_fmt})
                
            st.download_button(
                label="📥 下載完整 Excel 視覺化報表",
                data=output.getvalue(),
                file_name=f"CBAS_Report_{datetime.now().strftime('%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            status.update(label="🏁 今日無爆量訊號", state="complete")
            st.info("今日盤面平穩，無異常爆量標的。")
