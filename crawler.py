import requests
from bs4 import BeautifulSoup
import json
import logging
import time
import datetime
import os

# 从环境变量获取企业微信Webhook
WECHAT_WORK_WEBHOOK = os.getenv("WECHAT_WORK_WEBHOOK")
KEYWORDS = ["敗北","全"]

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
        matched_books = []
        publish_months = set()
        has_matched = False
        
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
                    # 转换为小写进行匹配（忽略大小写）
                    if keyword.lower() in title.lower():
                        matched_books.append({
                            "title": title,
                            "keyword": keyword,
                            "publish_month": publish_month,
                            "execute_time": execute_time
                        })
                        has_matched = True
                        logging.info(f"匹配到书籍: {title} (关键词: {keyword}, 出版月份: {publish_month})")
        
        # 生成月份范围（避免引用未定义变量）
        month_range = "、".join(sorted(publish_months)) if publish_months else "未知月份"
        
        # 发送企业微信通知
        if matched_books:
            total_books = len(matched_books)
            content = ""
            
            # 按月份分组
            books_by_month = {}
            for book in matched_books:
                month = book['publish_month']
                if month not in books_by_month:
                    books_by_month[month] = []
                books_by_month[month].append(book)
            
            # 按月份生成内容
            for month, books in books_by_month.items():
                content += f"## 📅 {month} 出版的匹配书籍\n\n"
                for i, book in enumerate(books, 1):
                    content += f"### {i}. {book['title']}\n"
                    content += f"- 关键词: `{book['keyword']}`\n\n"
            
            notification_title = f"📚 {execute_time} 发现{total_books}本包含关键词的书籍 ({month_range})"
            
            # 发送通知（支持分段）
            if send_wechat_notification(notification_title, content):
                logging.info(f"成功发现{total_books}本匹配书籍并推送通知")
            else:
                logging.error("推送通知失败")
                
        else:
            # 未匹配到书籍
            if not has_matched and len(publish_months) > 0:
                # 未匹配到书籍，但爬取成功
                content = f"今日未找到包含关键词 `{'、'.join(KEYWORDS)}` 的书籍。\n\n"
                content += f"📅 当前查询月份: {month_range}\n"
                content += f"🕒 检测时间: {execute_time}\n"
                send_wechat_notification(f"📚 {execute_time} 未发现匹配的书籍 ({month_range})", content)
                logging.info("未发现匹配的书籍")
            else:
                # 爬取出错或无数据
                error_content = f"可能原因:\n"
                error_content += f"1. 关键词 `{'、'.join(KEYWORDS)}` 不存在\n"
                error_content += f"2. 网站结构变化导致爬取失败\n"
                error_content += f"3. 网络请求超时\n\n"
                error_content += f"🕒 检测时间: {execute_time}\n"
                send_wechat_notification(f"⚠️ {execute_time} 爬虫执行异常", error_content)
                logging.warning("未获取到有效数据或爬取失败")
                
    except Exception as e:
        logging.error(f"执行爬虫时发生错误: {str(e)}")
        error_content = f"错误信息: `{str(e)}`\n\n"
        error_content += f"🕒 错误时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        send_wechat_notification(f"❌ {execute_time} 爬虫执行失败", error_content)
        raise  # 让GitHub Actions标记任务失败

if __name__ == "__main__":
    main()
