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
DB_FILE = "book_history.db"  # 存储历史数据的SQLite数据库文件
MAX_MESSAGES_PER_DAY = 3  # 每日最多发送的消息数
MIN_INTERVAL_BETWEEN_MESSAGES = 60 * 10  # 消息之间的最小间隔（秒）
LAST_MESSAGE_TIME_FILE = "last_message_time.txt"  # 记录上次发送消息的时间

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

def get_last_message_time():
    """获取上次发送消息的时间"""
    try:
        if os.path.exists(LAST_MESSAGE_TIME_FILE):
            with open(LAST_MESSAGE_TIME_FILE, 'r') as f:
                timestamp = float(f.read().strip())
                return datetime.datetime.fromtimestamp(timestamp)
        return None
    except Exception as e:
        logging.error(f"读取上次发送消息时间时出错: {str(e)}")
        return None

def save_last_message_time():
    """保存当前时间为上次发送消息的时间"""
    try:
        with open(LAST_MESSAGE_TIME_FILE, 'w') as f:
            f.write(str(time.time()))
    except Exception as e:
        logging.error(f"保存上次发送消息时间时出错: {str(e)}")

def send_combined_message(title, content):
    """发送合并后的企业微信消息（考虑API限制）"""
    if not WECHAT_WORK_WEBHOOK:
        logging.warning("未设置企业微信Webhook，无法发送通知")
        return False
    
    # 检查上次发送时间，确保符合最小间隔要求
    last_time = get_last_message_time()
    now = datetime.datetime.now()
    
    if last_time and (now - last_time).total_seconds() < MIN_INTERVAL_BETWEEN_MESSAGES:
        wait_time = MIN_INTERVAL_BETWEEN_MESSAGES - (now - last_time).total_seconds()
        logging.info(f"距离上次发送消息时间不足，等待 {wait_time:.0f} 秒")
        time.sleep(wait_time)
    
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "markdown",
        "markdown": {
            "content": content
        }
    }
    
    try:
        # 重试机制
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(WECHAT_WORK_WEBHOOK, headers=headers, 
                                        data=json.dumps(data), timeout=15)
                response.raise_for_status()
                
                result = response.json()
                if result.get("errcode") == 0:
                    logging.info(f"企业微信通知发送成功: {title}")
                    save_last_message_time()  # 保存发送时间
                    return True
                else:
                    logging.error(f"企业微信API返回错误: {result}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # 指数退避
                        logging.info(f"尝试重试 ({attempt+1}/{max_retries})，等待 {wait_time} 秒")
                        time.sleep(wait_time)
                    else:
                        return False
            except requests.exceptions.RequestException as e:
                logging.error(f"发送请求时出错: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    logging.info(f"尝试重试 ({attempt+1}/{max_retries})，等待 {wait_time} 秒")
                    time.sleep(wait_time)
                else:
                    return False
            
        return False
    except Exception as e:
        logging.error(f"发送企业微信通知时出错: {str(e)}")
        return False

def send_wechat_notification(title, content):
    """发送企业微信机器人通知（合并所有内容，考虑API限制）"""
    # 企业微信Markdown消息最大长度限制（约4000字符）
    MAX_LENGTH = 3800
    markdown_content = f"# {title}\n\n{content}\n\n> 来自 GitHub Actions 爬虫任务"
    
    # 如果内容过长，分段发送（但尽量合并为一条）
    if len(markdown_content) > MAX_LENGTH:
        logging.info(f"消息长度 {len(markdown_content)} 超过限制，将分段发送")
        
        # 尝试智能分段，保持章节完整性
        sections = []
        current_section = ""
        
        # 按章节分割（假设内容中有##标记的章节）
        chapter_sections = markdown_content.split("\n\n## ")
        
        for i, section in enumerate(chapter_sections):
            if i == 0:  # 第一个部分（标题和引言）
                current_section = section
            else:
                # 检查添加当前章节后是否会超过限制
                if len(current_section) + len(section) + len("\n\n## ") < MAX_LENGTH:
                    current_section += "\n\n## " + section
                else:
                    # 如果当前章节单独就超过限制，强制分割
                    if len(section) > MAX_LENGTH:
                        # 按子章节分割（假设内容中有###标记的子章节）
                        sub_sections = section.split("\n\n### ")
                        for j, sub_section in enumerate(sub_sections):
                            if j == 0:  # 第一个子章节
                                if len(current_section) + len(sub_section) + len("\n\n### ") < MAX_LENGTH:
                                    current_section += "\n\n### " + sub_section
                                else:
                                    sections.append(current_section)
                                    current_section = "## " + sub_section
                            else:
                                if len(current_section) + len(sub_section) + len("\n\n### ") < MAX_LENGTH:
                                    current_section += "\n\n### " + sub_section
                                else:
                                    sections.append(current_section)
                                    current_section = "## " + sub_section
                    else:
                        # 否则，将当前章节作为新的部分
                        sections.append(current_section)
                        current_section = "## " + section
        
        # 添加最后一个部分
        if current_section:
            sections.append(current_section)
        
        # 发送所有部分
        success = True
        for i, section in enumerate(sections):
            if i == 0:
                section_title = title
            else:
                section_title = f"{title} (续{i})"
            
            if not send_combined_message(section_title, section):
                success = False
        
        return success
    else:
        # 内容未超过限制，直接发送
        return send_combined_message(title, markdown_content)

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
            
            # 记录所有书籍
            for title in titles:
                book_info = {
                    "title": title,
                    "publish_month": publish_month
                }
                current_books.append(book_info)
                
                if not check_book_exists(title):
                    # 新书：添加到数据库并记录
                    if add_book(title, publish_month, today, today):
                        new_books.append({
                            "title": title,
                            "publish_month": publish_month,
                            "first_seen": today
                        })
                        logging.info(f"发现新书: {title} (出版月份: {publish_month})")
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
            notification_content += f"🎉 **今日发现 {total_new_books} 本预定出书** 🎉\n\n"
            
            # 按月份分组
            books_by_month = {}
            for book in new_books:
                month = book['publish_month']
                if month not in books_by_month:
                    books_by_month[month] = []
                books_by_month[month].append(book)
            
            # 按月份生成内容
            for month, books in books_by_month.items():
                notification_content += f"## 📅 {month} 预定出版的新书\n\n"
                for i, book in enumerate(books, 1):
                    notification_content += f"### {i}. {book['title']}\n"
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
            
            # 发送合并后的通知（考虑API限制）
            if send_wechat_notification(notification_title, notification_content):
                logging.info(f"成功推送合并后的更新通知: {len(new_books)}本预定出书, {len(published_books)}本已出书")
            else:
                logging.error("推送合并后的更新通知失败")
        else:
            # 没有新书和已出书
            content = f"今日没有发现新的预定出书，也没有书籍标记为已出版。\n\n"
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
