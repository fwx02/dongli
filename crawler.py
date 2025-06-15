import requests
from bs4 import BeautifulSoup
import json
import logging
import time
import datetime
import os
import sqlite3
import re

# 配置参数
DB_DIR = os.getenv("DB_DIR", ".")
DB_FILE = os.path.join(DB_DIR, "book_history.db")
LOG_FILE = os.path.join(DB_DIR, "crawler.log")
WECHAT_WORK_WEBHOOK = os.getenv("WECHAT_WORK_WEBHOOK")
MAX_MESSAGE_LENGTH = 3500
MAX_BOOKS_PER_BATCH = 15
DB_COMMIT_BATCH_SIZE = 10
MIN_INTERVAL_BETWEEN_MESSAGES = 3
LAST_MESSAGE_TIME_FILE = os.path.join(DB_DIR, "last_message_time.txt")

def setup_logging():
    """配置日志记录"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info(f"数据库文件: {DB_FILE}")
    logging.info(f"日志文件: {LOG_FILE}")

def normalize_title(title):
    """规范化书籍标题，提高查重准确性"""
    original_title = title
    title = title.lstrip('0123456789.、 ')  # 移除行号前缀
    title = re.sub(r'[ \t]+', ' ', title)  # 合并连续空格
    
    # 选择性移除版本信息（保留主要标题）
    title = re.sub(r'（首刷.*?）', '', title)  # 移除首刷限定等信息
    title = re.sub(r'\(首刷.*?\)', '', title)
    
    title = title.strip()
    logging.debug(f"标题规范化: '{original_title}' → '{title}'")
    return title

def create_database():
    """创建数据库表（如果不存在）"""
    try:
        os.makedirs(DB_DIR, exist_ok=True)
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
        
        # 调试：验证表结构
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(books)")
        columns = cursor.fetchall()
        logging.debug(f"表结构: {columns}")
        conn.close()
    except Exception as e:
        logging.error(f"创建数据库时出错: {str(e)}")
        raise

def check_book_exists(title):
    """检查书籍是否已存在于数据库中"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM books WHERE title = ?", (title,))
        result = cursor.fetchone()
        conn.close()
        
        # 调试：输出检查结果
        exists = "存在" if result else "不存在"
        logging.debug(f"检查书籍 '{title}': {exists}")
        return result is not None
    except Exception as e:
        logging.error(f"检查书籍时出错: {str(e)}")
        return False

def batch_add_books(books):
    """批量添加书籍到数据库"""
    if not books:
        logging.info("没有新书需要添加")
        return 0
    
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        new_count = 0
        
        for i, book in enumerate(books):
            normalized_title = normalize_title(book["title"])
            
            # 调试：输出处理中的书籍信息
            logging.debug(f"处理书籍 #{i+1}/{len(books)}: '{normalized_title}'")
            
            if not check_book_exists(normalized_title):
                try:
                    cursor.execute(
                        """INSERT INTO books (title, publish_month, first_seen, last_seen, is_published) 
                           VALUES (?, ?, ?, ?, 0)""",
                        (normalized_title, book["publish_month"], book["first_seen"], book["last_seen"])
                    )
                    new_count += 1
                    logging.info(f"✅ 新书入库: '{normalized_title}' ({book['publish_month']})")
                except sqlite3.IntegrityError as e:
                    logging.warning(f"⚠️ 书籍已存在或违反唯一约束: '{normalized_title}', 错误: {str(e)}")
                except Exception as e:
                    logging.error(f"❌ 添加书籍失败: '{normalized_title}', 错误: {str(e)}")
            else:
                logging.info(f"📚 书籍已存在: '{normalized_title}'")
            
            # 每批提交一次
            if (i + 1) % DB_COMMIT_BATCH_SIZE == 0:
                conn.commit()
                logging.debug(f"批量提交 {i+1} 条记录")
        
        conn.commit()
        conn.close()
        logging.info(f"✅ 共添加 {new_count} 本新书，处理 {len(books)} 本书籍")
        
        # 调试：验证插入结果
        if new_count > 0:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM books")
            count = cursor.fetchone()[0]
            logging.info(f"📊 数据库总记录数: {count}")
            conn.close()
        
        return new_count
    except Exception as e:
        logging.error(f"❌ 批量添加书籍时出错: {str(e)}")
        conn.rollback()
        conn.close()
        return 0

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
        
        # 调试：输出响应状态和部分内容
        logging.debug(f"响应状态码: {response.status_code}")
        logging.debug(f"响应内容前1000字符: {response.text[:1000]}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取出书表月份
        month_element = soup.select_one('h5.sdBook_t span#ContentPlaceHolder1_DataMonth')
        publish_month = month_element.get_text(strip=True) if month_element else "未知月份"
        logging.info(f"当前爬取的是 {publish_month} 的出书表")
        
        book_elements = soup.select('td[data-th="書名／集數"]')
        
        # 调试：输出提取到的书籍数量和样本
        logging.info(f"提取到 {len(book_elements)} 本书籍标题")
        if book_elements:
            sample_titles = [el.get_text(strip=True) for el in book_elements[:3]]
            logging.info(f"样本标题: {sample_titles}")
        
        # 处理书名，移除可能的行号前缀
        processed_titles = []
        for title in [element.get_text(strip=True) for element in book_elements]:
            processed_title = title.lstrip('0123456789.、 ')
            processed_titles.append(processed_title)
        
        return {
            "publish_month": publish_month,
            "titles": processed_titles
        }
    except Exception as e:
        logging.error(f"爬取第{page_num}页时出错: {str(e)}")
        return {
            "publish_month": "未知月份",
            "titles": []
        }

# 其他函数保持不变...

def main():
    """主函数"""
    try:
        setup_logging()
        logging.info("📖 爬虫程序启动")
        
        # 初始化数据库
        create_database()
        
        # 爬取数据
        new_books = []
        current_books = []
        publish_months = set()
        
        for page in range(1, 4):
            result = get_book_titles(page)
            titles = result["titles"]
            publish_month = result["publish_month"]
            publish_months.add(publish_month)
            
            # 记录所有书籍
            page_books = []
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            
            for title in titles:
                book_info = {
                    "title": title,
                    "publish_month": publish_month,
                    "first_seen": today,
                    "last_seen": today
                }
                page_books.append(book_info)
            
            current_books.extend(page_books)
            
            # 检查并记录新书
            for book in page_books:
                normalized_title = normalize_title(book["title"])
                if not check_book_exists(normalized_title):
                    new_books.append(book)
                    logging.info(f"🔍 发现新书: {normalized_title} ({publish_month})")
        
        # 调试：输出爬取结果
        logging.info(f"📊 爬取完成: {len(current_books)} 本当前书籍, {len(new_books)} 本新书")
        
        # 添加新书到数据库
        if new_books:
            added_count = batch_add_books(new_books)
            logging.info(f"✅ 数据库更新: 添加了 {added_count} 本新书")
        else:
            logging.info("📭 没有发现新书")
        
        # 其余代码保持不变...
        
    except Exception as e:
        logging.error(f"❌ 程序运行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()
