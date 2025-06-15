import requests
from bs4 import BeautifulSoup
import json
import logging
import time
import datetime
import os
import sqlite3

# 从环境变量获取企业微信Webhook
WECHAT_WORK_WEBHOOK = os.getenv("WECHAT_WORK_WEBHOOK")
KEYWORDS = ["敗北"]
DB_FILE = "book_history.db"  # 存储历史数据的SQLite数据库文件

def create_database():
    """创建数据库表（如果不存在）"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                title TEXT PRIMARY KEY,
                publish_month TEXT,
                first_seen TEXT,
                last_seen TEXT,
                is_published INTEGER DEFAULT 0
            )
        ''')
        conn.commit()
        conn.close()
        logging.info("数据库初始化完成")
    except Exception as e:
        logging.error(f"创建数据库时出错: {str(e)}")

def check_book_exists(title):
    """检查书籍是否已存在于数据库中"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM books WHERE title = ? AND is_published = 0", (title,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logging.error(f"检查书籍时出错: {str(e)}")
        return False

def add_book(title, publish_month, first_seen, last_seen):
    """将新书添加到数据库"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO books (title, publish_month, first_seen, last_seen, is_published) 
               VALUES (?, ?, ?, ?, 0)""",
            (title, publish_month, first_seen, last_seen)
        )
        conn.commit()
        conn.close()
        logging.info(f"新书已添加到数据库: {title}")
        return True
    except sqlite3.IntegrityError:
        # 书籍已存在，更新last_seen
        update_book_last_seen(title, last_seen)
        return False
    except Exception as e:
        logging.error(f"添加书籍时出错: {str(e)}")
        return False

def update_book_last_seen(title, last_seen):
    """更新书籍的最后一次出现时间"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE books SET last_seen = ? WHERE title = ? AND is_published = 0",
            (last_seen, title)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"更新书籍last_seen时出错: {str(e)}")

def mark_book_as_published(title, publish_date=None):
    """标记书籍为已出书"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # 如果提供了出版日期，更新publish_month
        if publish_date:
            cursor.execute(
                "UPDATE books SET is_published = 1, publish_month = ? WHERE title = ?",
                (publish_date, title)
            )
        else:
            cursor.execute(
                "UPDATE books SET is_published = 1 WHERE title = ?",
                (title,)
            )
        conn.commit()
        conn.close()
        logging.info(f"书籍已标记为已出书: {title}")
        return True
    except Exception as e:
        logging.error(f"标记书籍为已出书时出错: {str(e)}")
        return False

def get_unpublished_books():
    """获取所有未出书的书籍"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT title, publish_month, first_seen, last_seen FROM books WHERE is_published = 0")
        result = cursor.fetchall()
        conn.close()
        return [
            {
                "title": row[0],
                "publish_month": row[1],
                "first_seen": row[2],
                "last_seen": row[3]
            }
            for row in result
        ]
    except Exception as e:
        logging.error(f"获取未出书书籍时出错: {str(e)}")
        return []

def check_and_mark_published_books(current_books):
    """检查并标记已出书的书籍"""
    try:
        current_titles = [book["title"] for book in current_books]
        unpublished_books = get_unpublished_books()
        
        published_books = []
        for book in unpublished_books:
            if book["title"] not in current_titles:
                if mark_book_as_published(book["title"]):
                    published_books.append(book)
        
        return published_books
    except Exception as e:
        logging.error(f"检查并标记已出书书籍时出错: {str(e)}")
        return []

def send_single_message(title, content):
    """发送单条企业微信消息"""
    if not WECHAT_WORK_WEBHOOK:
        logging.warning("未设置企业微信Webhook，无法发送通知")
        return False
    
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "markdown",
        "markdown": {
            "content": content
        }
    }
    
    try:
        response = requests.post(WECHAT_WORK_WEBHOOK, headers=headers, 
                                data=json.dumps(data), timeout=15)
        response.raise_for_status()
        
        result = response.json()
        if result.get("errcode") == 0:
            logging.info(f"企业微信通知发送成功: {title}")
            return True
        else:
            logging.error(f"企业微信API返回错误: {result}")
            return False
            
    except Exception as e:
        logging.error(f"发送企业微信通知时出错: {str(e)}")
        return False

def send_wechat_notification(title, content):
    """发送企业微信机器人通知（支持长消息分段发送）"""
    # 企业微信Markdown消息最大长度限制（约4000字符）
    MAX_LENGTH = 3800
    markdown_content = f"# {title}\n\n{content}\n\n> 来自 GitHub Actions 爬虫任务"
    
    # 如果内容过长，分段发送
    if len(markdown_content) > MAX_LENGTH:
        logging.info(f"消息长度 {len(markdown_content)} 超过限制，将分段发送")
        
        # 按章节分割内容（假设内容中有###标记的章节）
        sections = markdown_content.split("\n\n### ")
        
        # 发送标题和第一段
        first_section = sections[0]
        if not send_single_message(title, first_section):
            return False
        
        # 发送剩余章节
        for i, section in enumerate(sections[1:], 1):
            section_title = f"{title} (续{i})"
            section_content = f"### {section}"
            
            # 如果单节内容仍过长，再次分割
            if len(section_content) > MAX_LENGTH:
                # 按行分割（每行一本书）
                lines = section_content.split("\n")
                sub_sections = []
                current_section = ""
                
                for line in lines:
                    if len(current_section) + len(line) + 1 < MAX_LENGTH:
                        current_section += line + "\n"
                    else:
                        sub_sections.append(current_section)
                        current_section = line + "\n"
                
                if current_section:
                    sub_sections.append(current_section)
                
                # 发送子章节
                for j, sub_section in enumerate(sub_sections, 1):
                    sub_title = f"{section_title} (部分{j})"
                    if not send_single_message(sub_title, sub_section):
                        return False
                    time.sleep(1)  # 避免频率限制
            else:
                # 单节内容未超过限制，直接发送
                if not send_single_message(section_title, section_content):
                    return False
                time.sleep(1)  # 避免频率限制
                
        return True
    else:
        # 内容未超过限制，直接发送
        return send_single_message(title, markdown_content)

def get_book_titles(page_num):
    """获取指定页的书籍标题和出版时间"""
    url = f"https://www.tongli.com.tw/Search1.aspx?Page={page_num}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        logging.info(f"开始爬取第{page_num}页: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取出书表月份
        month_element = soup.select_one('h5.sdBook_t span#ContentPlaceHolder1_DataMonth')
        publish_month = month_element.get_text(strip=True) if month_element else "未知月份"
        logging.info(f"当前爬取的是 {publish_month} 的出书表")
        
        book_elements = soup.select('td[data-th="書名／集數"]')
        
        # 添加调试信息
        logging.info(f"页面状态码: {response.status_code}")
        logging.info(f"提取到 {len(book_elements)} 本书籍标题")
        
        # 输出前3个标题（调试用）
        if book_elements:
            sample_titles = [element.get_text(strip=True) for element in book_elements[:3]]
            logging.info(f"示例标题: {sample_titles}")
        
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
        new_books = []  # 存储新发现的书籍
        published_books = []  # 存储已出书的书籍
        publish_months = set()
        
        # 记录执行时间（转换为北京时间 UTC+8）
        utc_time = datetime.datetime.utcnow()
        beijing_time = utc_time + datetime.timedelta(hours=8)
        execute_time = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        today = beijing_time.strftime("%Y-%m-%d")
        logging.info(f"定时任务执行时间: {execute_time} (北京时间)")
        
        # 初始化数据库
        create_database()
        
        # 爬取三页数据
        current_books = []
        for page in range(1, 4):
            result = get_book_titles(page)
            titles = result["titles"]
            publish_month = result["publish_month"]
            publish_months.add(publish_month)
            
            logging.info(f"第{page}页获取到{len(titles)}本书籍标题 ({publish_month})")
            
            # 筛选包含关键词的书籍，并检查是否为新书
            for title in titles:
                for keyword in KEYWORDS:
                    # 转换为小写进行匹配（忽略大小写）
                    if keyword.lower() in title.lower():
                        book_info = {
                            "title": title,
                            "publish_month": publish_month,
                            "keyword": keyword
                        }
                        current_books.append(book_info)
                        
                        if not check_book_exists(title):
                            # 新书：添加到数据库并记录
                            if add_book(title, publish_month, today, today):
                                new_books.append({
                                    "title": title,
                                    "keyword": keyword,
                                    "publish_month": publish_month,
                                    "first_seen": today
                                })
                                logging.info(f"发现新书: {title} (关键词: {keyword}, 出版月份: {publish_month})")
                        else:
                            # 已存在的书籍：更新last_seen
                            update_book_last_seen(title, today)
                            logging.info(f"已存在的书籍: {title}")
        
        # 生成月份范围
        month_range = "、".join(sorted(publish_months)) if publish_months else "未知月份"
        
        # 每天检查并标记已出书的书籍
        logging.info("执行每日已出书书籍检查")
        published_books = check_and_mark_published_books(current_books)
        logging.info(f"共发现{len(published_books)}本已出书的书籍")
        
        # 发送企业微信通知
        notification_content = ""
        
        # 添加新书信息
        if new_books:
            total_new_books = len(new_books)
            notification_content += f"🎉 **今日发现 {total_new_books} 本新书** 🎉\n\n"
            
            # 按月份分组
            books_by_month = {}
            for book in new_books:
                month = book['publish_month']
                if month not in books_by_month:
                    books_by_month[month] = []
                books_by_month[month].append(book)
            
            # 按月份生成内容
            for month, books in books_by_month.items():
                notification_content += f"## 📅 {month} 出版的新书\n\n"
                for i, book in enumerate(books, 1):
                    notification_content += f"### {i}. {book['title']}\n"
                    notification_content += f"- 关键词: `{book['keyword']}`\n"
                    notification_content += f"- 首次发现: `{book['first_seen']}`\n\n"
        
        # 添加已出书信息
        if published_books:
            total_published_books = len(published_books)
            notification_content += f"📦 **今日有 {total_published_books} 本书已出版** 📦\n\n"
            
            # 按月份分组
            published_by_month = {}
            for book in published_books:
                month = book['publish_month']
                if month not in published_by_month:
                    published_by_month[month] = []
                published_by_month[month].append(book)
            
            # 按月份生成内容
            for month, books in published_by_month.items():
                notification_content += f"## 📅 {month} 已出版的书籍\n\n"
                for i, book in enumerate(books, 1):
                    notification_content += f"### {i}. {book['title']}\n"
                    notification_content += f"- 首次出现: `{book['first_seen']}`\n"
                    notification_content += f"- 最后出现: `{book['last_seen']}`\n"
                    notification_content += f"- 状态: ✅ **已出版**\n\n"
        
        # 如果有新书或已出书，发送综合通知
        if new_books or published_books:
            notification_title = f"📚 {execute_time} 书籍更新 ({month_range})"
            
            # 发送通知（支持分段）
            if send_wechat_notification(notification_title, notification_content):
                logging.info(f"成功推送更新通知: {len(new_books)}本新书, {len(published_books)}本已出书")
            else:
                logging.error("推送更新通知失败")
        else:
            # 没有新书和已出书
            content = f"今日没有发现包含关键词 `{'、'.join(KEYWORDS)}` 的新书，也没有书籍标记为已出版。\n\n"
            content += f"📅 当前查询月份: {month_range}\n"
            content += f"🕒 检测时间: {execute_time}\n"
            send_wechat_notification(f"📚 {execute_time} 无更新 ({month_range})", content)
            logging.info("今日无更新")
                
    except Exception as e:
        logging.error(f"执行爬虫时发生错误: {str(e)}")
        error_content = f"错误信息: `{str(e)}`\n\n"
        error_content += f"🕒 错误时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        send_wechat_notification(f"❌ {execute_time} 爬虫执行失败", error_content)
        raise  # 让GitHub Actions标记任务失败

if __name__ == "__main__":
    main()
