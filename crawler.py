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
    original_title = title
    title = title.lstrip('0123456789.、 ')  # 移除行号前缀
    title = re.sub(r'[ \t]+', ' ', title)  # 合并连续空格
    
    # 选择性移除版本信息（保留主要标题）
    title = re.sub(r'（首刷.*?）', '', title)  # 移除首刷限定等信息
    title = re.sub(r'\(首刷.*?\)', '', title)
    title = re.sub(r'【.*?】', '', title)  # 移除方括号内容
    
    title = title.strip()
    logging.debug(f"标题规范化: '{original_title}' → '{title}'")
    return title

def create_database():
    """创建数据库表（如果不存在）"""
    try:
        os.makedirs(DB_DIR, exist_ok=True)
        logging.info(f"数据库目录: {DB_DIR}")
        
        # 验证目录权限
        if not os.access(DB_DIR, os.W_OK):
            logging.error(f"目录不可写: {DB_DIR}")
            raise PermissionError(f"无法写入目录: {DB_DIR}")
        
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
        
        # 验证表是否创建成功
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='books'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            logging.info("✅ 书籍表创建成功")
        else:
            logging.error("❌ 书籍表创建失败")
            raise Exception("数据库表创建失败")
        
        conn.commit()
        conn.close()
        
        # 验证文件是否存在
        if os.path.exists(DB_FILE):
            logging.info(f"✅ 数据库文件已创建: {DB_FILE}")
            file_permissions = oct(os.stat(DB_FILE).st_mode & 0o777)
            logging.info(f"文件权限: {file_permissions}")
        else:
            logging.error(f"❌ 数据库文件不存在: {DB_FILE}")
            raise FileNotFoundError("数据库文件未创建")
            
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
            
            if (i + 1) % DB_COMMIT_BATCH_SIZE == 0:
                conn.commit()
                logging.debug(f"批量提交 {i+1} 条记录")
        
        conn.commit()
        conn.close()
        logging.info(f"✅ 共添加 {new_count} 本新书，处理 {len(books)} 本书籍")
        
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
        
        logging.debug(f"响应状态码: {response.status_code}")
        logging.debug(f"响应内容前1000字符: {response.text[:1000]}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        month_element = soup.select_one('h5.sdBook_t span#ContentPlaceHolder1_DataMonth')
        publish_month = month_element.get_text(strip=True) if month_element else "未知月份"
        logging.info(f"当前爬取的是 {publish_month} 的出书表")
        
        book_elements = soup.select('td[data-th="書名／集數"]')
        
        logging.info(f"提取到 {len(book_elements)} 本书籍标题")
        if book_elements:
            sample_titles = [el.get_text(strip=True) for el in book_elements[:3]]
            logging.info(f"样本标题: {sample_titles}")
        
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

def test_database_creation():
    """测试数据库创建功能"""
    try:
        logging.info("开始测试数据库创建...")
        
        # 检查目录权限
        if not os.access(DB_DIR, os.W_OK):
            logging.error(f"目录不可写: {DB_DIR}")
            return False
        
        # 创建测试文件
        test_file = os.path.join(DB_DIR, "test.txt")
        with open(test_file, 'w') as f:
            f.write("测试文件")
        
        if os.path.exists(test_file):
            logging.info("✅ 测试文件创建成功")
            os.remove(test_file)
        else:
            logging.error("❌ 无法创建测试文件")
            return False
        
        # 创建数据库
        create_database()
        
        # 验证数据库文件
        if os.path.exists(DB_FILE):
            logging.info("✅ 数据库文件存在")
            
            # 简单查询测试
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            conn.close()
            
            if result:
                logging.info("✅ 数据库查询测试成功")
                return True
            else:
                logging.error("❌ 数据库查询测试失败")
                return False
        else:
            logging.error("❌ 数据库文件不存在")
            return False
            
    except Exception as e:
        logging.error(f"数据库测试出错: {str(e)}")
        return False

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
    
    # 构建完整的请求JSON并计算其字节长度
    data = {
        "msgtype": "markdown",
        "markdown": {
            "content": content
        }
    }
    json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
    json_length = len(json_data)
    
    # 检查JSON序列化后的总长度
    if json_length > 4096:
        logging.warning(f"完整JSON请求长度 {json_length} 超过企业微信限制 4096 字节")
        
        # 尝试智能分段
        sections = split_message_smart(content)
        success = True
        
        for i, section in enumerate(sections):
            section_title = f"{title} (分段{i+1}/{len(sections)})"
            
            # 重新计算分段后的JSON长度
            section_data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": section
                }
            }
            section_length = len(json.dumps(section_data, ensure_ascii=False).encode('utf-8'))
            
            logging.info(f"发送分段 {i+1}/{len(sections)}: {section_title}，长度 {section_length} 字节")
            
            if section_length > 4096:
                logging.error(f"分段 {i+1} 长度 {section_length} 仍然超过限制，尝试截断")
                section = truncate_message(section)
                section_data["markdown"]["content"] = section
                section_length = len(json.dumps(section_data, ensure_ascii=False).encode('utf-8'))
                logging.info(f"截断后分段 {i+1} 长度为 {section_length} 字节")
            
            try:
                response = requests.post(
                    WECHAT_WORK_WEBHOOK, 
                    headers={'Content-Type': 'application/json'}, 
                    data=json.dumps(section_data, ensure_ascii=False), 
                    timeout=15
                )
                response.raise_for_status()
                
                result = response.json()
                if result.get("errcode") == 0:
                    logging.info(f"分段 {i+1} 发送成功")
                else:
                    logging.error(f"分段 {i+1} 发送失败: {result}")
                    success = False
            except Exception as e:
                logging.error(f"发送分段 {i+1} 时出错: {str(e)}")
                success = False
        
        if success:
            logging.info(f"消息已成功分段发送，共 {len(sections)} 段")
            save_last_message_time()
            return True
        else:
            logging.error("消息分段发送失败")
            return False
    else:
        # 直接发送消息
        logging.info(f"发送消息: {title}，长度 {json_length} 字节")
        try:
            response = requests.post(
                WECHAT_WORK_WEBHOOK, 
                headers={'Content-Type': 'application/json'}, 
                data=json_data, 
                timeout=15
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get("errcode") == 0:
                logging.info("消息发送成功")
                save_last_message_time()
                return True
            else:
                logging.error(f"消息发送失败: {result}")
                return False
        except Exception as e:
            logging.error(f"发送消息时出错: {str(e)}")
            return False

def split_message_smart(content):
    """智能分割长消息，保持内容完整性"""
    if len(content) <= MAX_MESSAGE_LENGTH:
        return [content]
    
    sections = []
    current_section = ""
    lines = content.split('\n')
    
    for line in lines:
        # 如果添加当前行后超过最大长度，则创建新的分段
        if len(current_section) + len(line) + 1 > MAX_MESSAGE_LENGTH:
            # 如果当前分段为空，强制添加此行（可能会超过限制，但这是极端情况）
            if not current_section:
                sections.append(line)
                current_section = ""
            else:
                sections.append(current_section)
                current_section = line
        else:
            # 添加当前行到当前分段
            if current_section:
                current_section += '\n' + line
            else:
                current_section = line
    
    # 添加最后一个分段
    if current_section:
        sections.append(current_section)
    
    return sections

def truncate_message(content):
    """截断消息内容，确保不超过最大长度"""
    if len(content) <= MAX_MESSAGE_LENGTH:
        return content
    
    # 尝试在最后一个完整的项目符号或标题处截断
    markers = ['\n- ', '\n* ', '\n# ', '\n## ', '\n### ']
    truncate_index = -1
    
    for marker in markers:
        index = content.rfind(marker, 0, MAX_MESSAGE_LENGTH)
        if index > truncate_index:
            truncate_index = index
    
    if truncate_index > 0:
        # 在标记后添加省略号
        return content[:truncate_index] + "\n...（消息过长，已截断）"
    else:
        # 没有找到合适的截断点，直接截断
        return content[:MAX_MESSAGE_LENGTH - 10] + "...（消息过长，已截断）"

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

def check_and_mark_published_books(current_books):
    """检查并标记已出书的书籍"""
    try:
        current_titles = [normalize_title(book["title"]) for book in current_books]
        unpublished_books = get_unpublished_books()
        
        published_books = []
        for book in unpublished_books:
            if normalize_title(book["title"]) not in current_titles:
                if mark_book_as_published(book["title"]):
                    published_books.append(book)
        
        return published_books
    except Exception as e:
        logging.error(f"检查并标记已出书书籍时出错: {str(e)}")
        return []

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

def mark_book_as_published(title, publish_date=None):
    """标记书籍为已出书"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
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

def main():
    """主函数"""
    try:
        setup_logging()
        logging.info("📖 爬虫程序启动")
        
        # 先运行数据库测试
        if not test_database_creation():
            logging.error("❌ 数据库测试失败，程序终止")
            return
        
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
        
        # 检查并标记已出书的书籍
        published_books = check_and_mark_published_books(current_books)
        if published_books:
            logging.info(f"📦 发现 {len(published_books)} 本已出书")
            
            # 构建已出书通知消息
            publish_message = "### 📦 新书到货通知\n\n"
            for book in published_books:
                publish_message += f"- **{book['title']}** (预计出版月份: {book['publish_month']})\n"
            
            # 发送通知
            if WECHAT_WORK_WEBHOOK:
                send_combined_message("新书到货通知", publish_message)
        else:
            logging.info("📭 没有书籍标记为已出书")
        
        # 获取所有未出版的书籍
        all_unpublished_books = get_unpublished_books()
        logging.info(f"📚 目前共有 {len(all_unpublished_books)} 本未出版的书籍")
        
        # 按出版月份分组
        books_by_month = {}
        for book in all_unpublished_books:
            month = book["publish_month"]
            if month not in books_by_month:
                books_by_month[month] = []
            books_by_month[month].append(book)
        
        # 构建等待列表消息
        if books_by_month and WECHAT_WORK_WEBHOOK:
            waiting_message = "### 📚 待出版书籍列表\n\n"
            
            # 按月份排序
            sorted_months = sorted(books_by_month.keys(), key=lambda x: (x.split('~')[0], x))
            
            for month in sorted_months:
                books = books_by_month[month]
                waiting_message += f"#### {month} ({len(books)}本)\n"
                
                # 按首次发现时间排序
                books_sorted = sorted(books, key=lambda x: x["first_seen"])
                
                for i, book in enumerate(books_sorted, 1):
                    days_waiting = (datetime.datetime.now() - datetime.datetime.strptime(book["first_seen"], "%Y-%m-%d")).days
                    waiting_message += f"{i}. **{book['title']}** (等待{days_waiting}天)\n"
                
                waiting_message += "\n"
            
            # 发送等待列表通知
            send_combined_message("待出版书籍列表", waiting_message)
        
        logging.info("✅ 爬虫程序执行完成")
        
    except Exception as e:
        import traceback
        logging.error(f"❌ 程序崩溃: {str(e)}")
        logging.error(f"堆栈跟踪:\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    main()
