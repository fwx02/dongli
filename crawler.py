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
MIN_INTERVAL_BETWEEN_MESSAGES = 60  # 消息之间的最小间隔（秒）
LAST_MESSAGE_TIME_FILE = "last_message_time.txt"  # 记录上次发送消息的时间
MAX_MESSAGE_LENGTH = 3500  # 更保守的消息最大长度限制
MAX_BOOKS_PER_BATCH = 15  # 每个批次最多包含的书籍数量
DB_COMMIT_BATCH_SIZE = 10  # 数据库提交批次大小

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
        raise  # 向上抛出异常，终止程序执行

def check_book_exists(title):
    """检查书籍是否已存在于数据库中（无论是否已出版）"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM books WHERE title = ?", (title,))
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
        added_count = 0
        
        for i, book in enumerate(books):
            try:
                cursor.execute(
                    """INSERT INTO books (title, publish_month, first_seen, last_seen, is_published) 
                       VALUES (?, ?, ?, ?, 0)""",
                    (book["title"], book["publish_month"], book["first_seen"], book["last_seen"], 0)
                )
                added_count += 1
                
                # 每批提交一次，避免长时间事务
                if (i + 1) % DB_COMMIT_BATCH_SIZE == 0:
                    conn.commit()
                    logging.debug(f"已批量提交 {i+1} 条记录到数据库")
            except sqlite3.IntegrityError:
                # 书籍已存在，更新last_seen（仅当书籍未被标记为已出版时）
                update_book_last_seen(book["title"], book["last_seen"])
            except Exception as e:
                logging.error(f"添加书籍 {book['title']} 时出错: {str(e)}")
        
        conn.commit()  # 提交剩余的记录
        conn.close()
        logging.info(f"成功添加 {added_count} 本新书到数据库，共处理 {len(books)} 本书籍")
        return added_count
    except Exception as e:
        logging.error(f"批量添加书籍时出错: {str(e)}")
        conn.rollback()  # 回滚事务
        conn.close()
        return 0

def update_book_last_seen(title, last_seen):
    """更新书籍的最后一次出现时间（仅当书籍未被标记为已出版时）"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # 只更新未出版的书籍
        cursor.execute(
            "UPDATE books SET last_seen = ? WHERE title = ? AND is_published = 0",
            (last_seen, last_seen, title)
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
                response = requests.post(WECHAT_WORK_WEBHOOK, headers={'Content-Type': 'application/json'}, 
                                        data=json.dumps(section_data, ensure_ascii=False), timeout=15)
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
        logging.info(f"消息长度 {json_length} 字节，在限制范围内")
        try:
            response = requests.post(WECHAT_WORK_WEBHOOK, headers={'Content-Type': 'application/json'}, 
                                    data=json_data, timeout=15)
            response.raise_for_status()
            
            result = response.json()
            if result.get("errcode") == 0:
                logging.info(f"企业微信通知发送成功: {title}，长度 {json_length} 字节")
                save_last_message_time()  # 保存发送时间
                return True
            else:
                logging.error(f"企业微信API返回错误: {result}")
                return False
        except Exception as e:
            logging.error(f"发送企业微信通知时出错: {str(e)}")
            return False

def split_message_smart(content):
    """智能分割长消息，确保每段都在限制范围内"""
    # 按月份和书籍数量双重控制
    sections = []
    current_section = ""
    
    # 按月份分割内容
    month_sections = content.split("\n\n## ")
    
    for i, section in enumerate(month_sections):
        if i == 0:  # 第一个部分（标题和引言）
            current_section = section
        else:
            section_content = "## " + section
            
            # 尝试添加新部分并检查长度
            test_data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": current_section + "\n\n" + section_content
                }
            }
            test_length = len(json.dumps(test_data, ensure_ascii=False).encode('utf-8'))
            
            # 如果添加当前部分会超过最大长度限制
            if test_length > 4096:
                # 保存当前部分并开始新的部分
                sections.append(current_section)
                current_section = section_content
            else:
                # 继续添加到当前部分
                current_section += "\n\n" + section_content
    
    # 添加最后一个部分
    if current_section:
        sections.append(current_section)
    
    # 检查每个部分是否都符合要求，如有必要进一步分割
    final_sections = []
    for section in sections:
        section_data = {
            "msgtype": "markdown",
            "markdown": {
                "content": section
            }
        }
        section_length = len(json.dumps(section_data, ensure_ascii=False).encode('utf-8'))
        
        if section_length <= 4096:
            final_sections.append(section)
        else:
            logging.warning(f"部分内容长度 {section_length} 超过限制，需要进一步分割")
            # 尝试按书籍列表分割
            books = section.split("\n### ")
            current_batch = []
            current_batch_length = 0
            
            for book in books:
                book_data = {
                    "msgtype": "markdown",
                    "markdown": {
                        "content": "\n### ".join(current_batch + [book])
                    }
                }
                batch_length = len(json.dumps(book_data, ensure_ascii=False).encode('utf-8'))
                
                if batch_length > 4096 and current_batch:
                    final_sections.append("\n### ".join(current_batch))
                    current_batch = [book]
                else:
                    current_batch.append(book)
            
            if current_batch:
                final_sections.append("\n### ".join(current_batch))
    
    return final_sections

def truncate_message(content):
    """截断消息内容，确保在限制范围内"""
    # 估算内容部分可以容纳的长度
    estimated_content_length = 3800  # 留出296字节的安全余量
    
    # 按字节截断，确保不破坏UTF-8字符
    truncated_bytes = content.encode('utf-8')[:estimated_content_length]
    
    # 尝试解码，如果截断在多字节字符中间，可能会失败
    try:
        truncated_content = truncated_bytes.decode('utf-8')
    except UnicodeDecodeError:
        # 寻找最近的完整字符边界
        for i in range(len(truncated_bytes) - 1, -1, -1):
            try:
                truncated_content = truncated_bytes[:i].decode('utf-8')
                break
            except UnicodeDecodeError:
                continue
        else:
            truncated_content = ""
    
    # 确保在完整的一行结束
    truncated_content = truncated_content.rsplit('\n', 1)[0]
    truncated_content += "\n\n...（内容过长，已自动截断）"
    
    return truncated_content

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
        
        # 处理书名，移除可能的行号前缀
        processed_titles = []
        for title in [element.get_text(strip=True) for element in book_elements]:
            # 移除可能的行号前缀（如"1."、"1、"等）
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
            page_books = []
            for title in titles:
                book_info = {
                    "title": title,
                    "publish_month": publish_month,
                    "first_seen": today,
                    "last_seen": today
                }
                page_books.append(book_info)
            
            # 批量处理书籍数据
            current_books.extend(page_books)
            
            # 检查并记录新书（分批处理）
            batch_size = MAX_BOOKS_PER_BATCH
            for i in range(0, len(page_books), batch_size):
                batch = page_books[i:i+batch_size]
                new_batch = []
                
                for book in batch:
                    if not check_book_exists(book["title"]):
                        new_batch.append(book)
                        logging.info(f"发现新书: {book['title']} (出版月份: {publish_month})")
                
                if new_batch:
                    new_books.extend(new_batch)
            
            logging.info(f"第{page}页处理完成，发现 {len(new_batch)} 本新书")
        
        # 批量添加新书到数据库
        if new_books:
            added_count = batch_add_books(new_books)
            logging.info(f"共添加 {added_count} 本新书到数据库")
        else:
            logging.info("没有发现新书")
        
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
