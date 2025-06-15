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
DB_DIR = os.getenv("DB_DIR", ".")  # 默认使用当前目录
DB_FILE = os.path.join(DB_DIR, "book_history.db")
LOG_FILE = os.path.join(DB_DIR, "crawler.log")
WECHAT_WORK_WEBHOOK = os.getenv("WECHAT_WORK_WEBHOOK")
MAX_MESSAGE_LENGTH = 3500
MAX_BOOKS_PER_BATCH = 15
DB_COMMIT_BATCH_SIZE = 10
MIN_INTERVAL_BETWEEN_MESSAGES = 60
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
    title = title.lstrip('0123456789.、 ')  # 移除行号前缀
    title = re.sub(r'[ \t]+', ' ', title)  # 合并连续空格
    
    # 移除版本信息（可选，根据实际情况调整）
    title = re.sub(r'（.*?）', '', title)  # 移除中文括号内容
    title = re.sub(r'\(.*?\)', '', title)  # 移除英文括号内容
    title = re.sub(r'【.*?】', '', title)  # 移除方括号内容
    
    return title.strip()

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
        return result is not None
    except Exception as e:
        logging.error(f"检查书籍时出错: {str(e)}")
        return False

def batch_add_books(books):
    """批量添加书籍到数据库"""
    if not books:
        return 0
    
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        new_count = 0
        
        for i, book in enumerate(books):
            normalized_title = normalize_title(book["title"])
            logging.debug(f"检查书籍: {normalized_title}")
            
            if not check_book_exists(normalized_title):
                try:
                    cursor.execute(
                        """INSERT INTO books (title, publish_month, first_seen, last_seen, is_published) 
                           VALUES (?, ?, ?, ?, 0)""",
                        (normalized_title, book["publish_month"], book["first_seen"], book["last_seen"])
                    )
                    new_count += 1
                    logging.info(f"新书入库: {normalized_title} ({book['publish_month']})")
                except sqlite3.IntegrityError:
                    logging.warning(f"书籍已存在: {normalized_title}")
                except Exception as e:
                    logging.error(f"添加书籍失败: {normalized_title}, 错误: {str(e)}")
            
            # 每批提交一次
            if (i + 1) % DB_COMMIT_BATCH_SIZE == 0:
                conn.commit()
                logging.debug(f"批量提交 {i+1} 条记录")
        
        conn.commit()
        conn.close()
        logging.info(f"共添加 {new_count} 本新书，处理 {len(books)} 本书籍")
        return new_count
    except Exception as e:
        logging.error(f"批量添加书籍时出错: {str(e)}")
        conn.rollback()
        conn.close()
        return 0

# 其他函数保持不变...

def main():
    """主函数"""
    try:
        setup_logging()
        logging.info("爬虫程序启动")
        
        # 初始化数据库
        create_database()
        
        # 爬取数据并处理
        # ... 其余代码保持不变 ...
        
    except Exception as e:
        logging.error(f"程序运行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()
