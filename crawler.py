import requests
from bs4 import BeautifulSoup
import json
import logging
import time
import datetime
import os

# 从环境变量获取企业微信Webhook
WECHAT_WORK_WEBHOOK = os.getenv("WECHAT_WORK_WEBHOOK")
KEYWORDS = ["敗北","首刷"]

def send_wechat_notification(title, content):
    """发送企业微信机器人通知"""
    if not WECHAT_WORK_WEBHOOK:
        logging.warning("未设置企业微信Webhook，无法发送通知")
        return
    
    # 构建Markdown消息（支持富文本格式）
    markdown_content = f"""
# {title}

{content}

> 来自 GitHub Actions 爬虫任务
"""
    
    # 发送POST请求到企业微信API
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "markdown",
        "markdown": {
            "content": markdown_content
        }
    }
    
    try:
        response = requests.post(WECHAT_WORK_WEBHOOK, headers=headers, data=json.dumps(data), timeout=10)
        response.raise_for_status()
        logging.info("企业微信通知发送成功")
    except Exception as e:
        logging.error(f"发送企业微信通知时出错: {str(e)}")

def get_book_titles(page_num):
    """获取指定页的书籍标题和出版时间"""
    url = f"https://www.tongli.com.tw/Search1.aspx?Page={page_num}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取出书表月份
        month_element = soup.select_one('h5.sdBook_t span#ContentPlaceHolder1_DataMonth')
        publish_month = month_element.get_text(strip=True) if month_element else "未知月份"
        logging.info(f"当前爬取的是 {publish_month} 的出书表")
        
        book_elements = soup.select('td[data-th="書名／集數"]')
        
        return {
            "publish_month": publish_month,
            "titles": [element.get_text(strip=True) for element in book_elements]
        }
    except Exception as e:
        logging.error(f"爬取第{page_num}页时出错: {str(e)}")
        return {
            "publish_month": "未知月份",
            "titles": []
        }

def main():
    """主函数（直接执行，无需云函数入口）"""
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        matched_books = []
        publish_months = set()
        
        # 记录执行时间（转换为北京时间 UTC+8）
        utc_time = datetime.datetime.utcnow()
        beijing_time = utc_time + datetime.timedelta(hours=8)
        execute_time = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"定时任务执行时间: {execute_time} (北京时间)")
        
        # 爬取三页数据
        for page in range(1, 4):
            result = get_book_titles(page)
            titles = result["titles"]
            publish_month = result["publish_month"]
            publish_months.add(publish_month)
            
            logging.info(f"第{page}页获取到{len(titles)}本书籍标题 ({publish_month})")
            
            # 筛选包含关键词的书籍
            for title in titles:
                for keyword in KEYWORDS:
                    if keyword in title:
                        matched_books.append({
                            "title": title,
                            "keyword": keyword,
                            "publish_month": publish_month,
                            "execute_time": execute_time
                        })
                        logging.info(f"匹配到书籍: {title} (关键词: {keyword}, 出版月份: {publish_month})")
        
        # 发送企业微信通知
        if matched_books:
            content = ""
            for i, book in enumerate(matched_books):
                content += f"### {i+1}. {book['title']}\n"
                content += f"关键词: `{book['keyword']}`\n"
                content += f"出版月份: `{book['publish_month']}`\n"
                content += f"检测时间: `{book['execute_time']}`\n\n"
            
            month_range = "、".join(sorted(publish_months))
            send_wechat_notification(f"📚 {execute_time} 发现{len(matched_books)}本包含关键词的书籍 ({month_range})", content)
            logging.info(f"成功发现{len(matched_books)}本匹配书籍并推送通知")
        else:
            month_range = "、".join(sorted(publish_months))
            content = f"今日未找到包含关键词 `{'、'.join(KEYWORDS)}` 的书籍。\n\n"
            content += f"📅 当前查询月份: {month_range}\n"
            content += f"🕒 检测时间: {execute_time}\n"
            send_wechat_notification(f"📚 {execute_time} 未发现匹配的书籍 ({month_range})", content)
            logging.info("未发现匹配的书籍")
            
    except Exception as e:
        logging.error(f"执行爬虫时发生错误: {str(e)}")
        if WECHAT_WORK_WEBHOOK:
            error_content = f"错误信息: `{str(e)}`\n\n"
            error_content += f"🕒 错误时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            send_wechat_notification(f"❌ 爬虫执行失败", error_content)
        raise  # 让GitHub Actions标记任务失败

if __name__ == "__main__":
    main()
