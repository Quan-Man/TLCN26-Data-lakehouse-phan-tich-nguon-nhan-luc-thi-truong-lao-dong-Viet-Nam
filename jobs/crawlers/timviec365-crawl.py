import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# CẤU HÌNH CHUNG
BASE_URL = "https://timviec365.vn"

OUTPUT_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "jobs"
    / f"timviec365_all_jobs_raw_"
      f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
)

MAX_RETRIES = 3

PAGE_TIMEOUT = 30000  # 30 seconds

# DANH SÁCH DANH MỤC CẦN CRAWL: 8
CATEGORIES = [
    {
        "category_name": "Điện - Điện tử",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-dien-dien-tu-c5v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-dien-dien-tu-c5v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "Cơ khí - Chế tạo",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-co-khi-che-tao-c11v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-co-khi-che-tao-c11v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "IT",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-it-phan-mem-c13v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-it-phan-mem-c13v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "Biên - Phiên dịch",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-bien-phien-dich-c22v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-bien-phien-dich-c22v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "Kiến trúc - Thiết kế nội thất",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-kien-truc-tk-noi-that-c24v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-kien-truc-tk-noi-that-c24v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "Thương mại điện tử",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-thuong-mai-dien-tu-c42v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-thuong-mai-dien-tu-c42v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "Luật - Pháp lý",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-luat-phap-ly-c53v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-luat-phap-ly-c53v0"
            "?page={page}"
        ),
    },

    {
        "category_name": "Môi trường - Xử lý chất thải",
        "base_url": (
            "https://timviec365.vn/"
            "viec-lam-moi-truong-xu-ly-chat-thai-c54v0"
        ),
        "page_url_template": (
            "https://timviec365.vn/"
            "viec-lam-moi-truong-xu-ly-chat-thai-c54v0"
            "?page={page}"
        ),
    },
]

# TẠO URL
def build_page_url(category,page_number):
    if page_number == 1:
        return category["base_url"]

    return category["page_url_template"].format(page=page_number)

# ============================================================
# TEXT HELPERS
# ============================================================
def clean_text(text):
    """
    Chuẩn hóa text một dòng.
    """

    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def clean_multiline_text(text):
    """
    Chuẩn hóa text nhiều dòng.
    """

    if not text: return ""

    lines = []

    for line in text.splitlines():
        line = re.sub(r"[ \t]+", " ", line).strip()

        if line:
            lines.append(line)

    return "\n".join(lines)

# LẤY TEXT AN TOÀN
async def get_text(locator, multiline=False):
    try:
        if await locator.count() == 0:
            return ""

        if multiline:
            text = await locator.first.text_content()
            return clean_multiline_text(text)

        text = await locator.first.inner_text()

        return clean_text(text)

    except Exception:
        return ""

# LẤY TRANG CUỐI ĐỂ XÁC ĐỊNH SỐ TRANG CẦN CRAWL
async def get_last_page(page) -> int:
    try:
        pagination_links = page.locator("li.pagi_pre_all a.link_page")
        count = await pagination_links.count()

        # Không có pagination
        if count == 0:
            print("Không có pagination.")
            print("last_page = 1")
            return 1

        page_numbers = set()

        # DUYỆT CÁC LINK
        for i in range(count):
            link = pagination_links.nth(i)

            # TEXT
            try:
                text = (await link.inner_text()).strip()

            except Exception:
                text = ""

            if text.isdigit():
                page_numbers.add(int(text))

            # HREF
            try:
                href = await link.get_attribute("href")

            except Exception:
                href = None

            if href:
                match = re.search(r"[?&]page=(\d+)", href)

                if match:
                    page_numbers.add(int(match.group(1)))

        if not page_numbers:
            print("Không tìm thấy số trang.")
            return 1

        last_page = max(page_numbers)
        return last_page

    except Exception as e:
        print(f"get_last_page error: {e}")
        return 1

# GET JOB CARDS
async def get_job_cards(page):
    selector = ("div.item_vl[data-newid]")
    cards = page.locator(selector)
    count = await cards.count()

    return cards, count

# PARSE JOB CARD
async def parse_job_card(card, category):
    try:
        # JOB ID
        job_id = await card.get_attribute("data-newid")

        if not job_id:
            return None

        # TITLE
        title_locator = card.locator("h2.box_title_new a.title_new")
        job_title = await get_text(title_locator)

        # URL
        href = await title_locator.get_attribute("href")

        if not href:
            return None

        job_url = urljoin(BASE_URL, href)

        # COMPANY
        company = await get_text(card.locator("a.name_com"))

        # LOCATION
        location = await get_text(card.locator(".job_city"))

        # SALARY
        salary = await get_text(card.locator(".job_money"))

        # EXPERIENCE
        experience = await get_text(card.locator(".item_catenew_exp"))
 
        # DEADLINE
        deadline = await get_text(card.locator(".job_time"))

        return {
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "salary": salary,
            "location": location,
            "experience": experience,
            "deadline": deadline,
            "job_url": job_url,
            "category_name": category["category_name"],
        }

    except Exception as e:
        print(f"Parse card error: {e}")
        return None

# DETAIL: LẤY THÔNG TIN THEO TITLE
async def get_detail_value(page, title):
    try:    
        items = page.locator("div.itemDetailInfo_center")
        count = await items.count()

        for i in range(count):
            item = items.nth(i)
            title_text = await get_text(item.locator(".titleContentSalary"))

            if (title_text.lower() == title.lower()):
                return await get_text(item.locator(".valContentSalary"))

    except Exception:
        pass

    return ""

# DETAIL: SECTION
async def get_detail_section(page, section_title):
    try:
        sections = page.locator("div.itemInfoSpecific")
        count = await sections.count()

        for i in range(count):
            section = sections.nth(i)

            title = await get_text(section.locator("h2.titleInfoSpecific"))

            if (title.lower() != section_title.lower()):
                continue

            content = await get_text(
                section.locator(
                    ".boxMainInfoSpecific"
                ),
                multiline=True
            )

            return content

    except Exception:
        pass

    return ""

# DETAIL: JOB INFORMATION
async def get_job_information(page):
    result = {}
    try:
        items = page.locator("div.itemYauCauKhac")
        count = await items.count()

        for i in range(count):
            item = items.nth(i)
            title = await get_text(item.locator(".titleYauCauKhac"))
            value = await get_text(item.locator(".valYauCauKhac"))

            if not title:
                continue

            if title == "Bằng cấp":
                result["education"] = value

            elif title == "Cập nhật":
                result["posted_at"] = value

    except Exception:
        pass

    return result

# CRAWL DETAIL
async def crawl_job_detail(context, job_basic, crawled_at):
    job_url = job_basic["job_url"]

    page = await context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT)
    success = False

    # RETRY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(
                job_url,
                wait_until="domcontentloaded",
                timeout=PAGE_TIMEOUT
            )

            await page.wait_for_timeout(1000) # 1 second

            # CHECK DETAIL

            if (await page.locator("h1.titleNew").count() == 0):
                print(f"[ERROR] Không thấy detail {attempt}/{MAX_RETRIES}")
                continue

            success = True
            break

        except PlaywrightTimeoutError:
            print(f"[ERROR] Timeout {attempt}/{MAX_RETRIES}")

        except Exception as e:
            print(f"[ERROR] Error {attempt}/{MAX_RETRIES}: {e}")

    # DETAIL FAILED
    if not success:
        await page.close()
        return None

    try:
        # TITLE

        job_title = await get_text(page.locator("h1.titleNew"))

        if not job_title:
            job_title = job_basic["job_title"]

        # SALARY
        salary = await get_detail_value(page,"Mức lương")

        if not salary:
            salary = job_basic["salary"]

        # LOCATION
        location = await get_detail_value(page, "Địa điểm")

        if not location:
            location = job_basic["location"]

        # DEADLINE
        deadline = await get_detail_value(page, "Hạn nộp")

        if not deadline:
            deadline = job_basic["deadline"]

        # EXPERIENCE
        experience = await get_detail_value(page, "Kinh nghiệm")

        if not experience:
            experience = job_basic["experience"]

        # DESCRIPTION
        description = await get_detail_section(page, "Mô tả công việc")

        # REQUIREMENTS
        requirements = await get_detail_section(page, "Yêu cầu")

        # JOB INFORMATION
        job_information = (await get_job_information(page))

        # SKILLS
        skills = []

        # SUMMARY
        job = {
            "category": job_basic["category_name"],

            "job_id":job_basic["job_id"],

            "job_title": job_title,

            "company": job_basic["company"],

            "salary": salary,

            "location": location,

            "deadline": deadline,

            "posted_at": job_information.get("posted_at", ""),

            "job_url": job_url,

            "experience": experience,

            "education": job_information.get("education", ""),

            "skills": skills,

            "description": description,

            "requirements": requirements,

            "source_name": "timviec365",

            "crawled_at": crawled_at,
        }

        print(f"SUCCESS: {job_title}")
        return job

    except Exception as e:
        print(
            f"Detail error "
            f"{job_basic['job_id']}: {e}"
        )

        return None

    finally:
        await page.close()

# CRAWL ONE CATEGORY PAGE
async def crawl_category_page(page, category, page_number):
    page_url = build_page_url(category, page_number)

    print()
    print("=" * 80)

    print(
        f"URL: "
        f"{page_url}"
    )

    success = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(
                page_url,
                wait_until="domcontentloaded",
                timeout=PAGE_TIMEOUT
            )

            await page.wait_for_timeout(1500) # 1.5 second

            # CHỜ JOB CARD
            await page.locator(
                "div.item_vl[data-newid]"
            ).first.wait_for(
                state="attached",
                timeout=30000
            )

            success = True
            break

        except PlaywrightTimeoutError:
            print(
                f"Timeout listing "
                f"{attempt}/{MAX_RETRIES}"
            )

        except Exception as e:
            print(
                f"Listing error "
                f"{attempt}/{MAX_RETRIES}: "
                f"{e}"
            )

    # FAILED
    if not success:
        try:
            html = await page.content()

            debug_file = (
                f"timviec365_debug_"
                f"{page_number}.html"
            )

            with open(
                debug_file,
                "w",
                encoding="utf-8"
            ) as f:

                f.write(html)

            print(f"Không lấy được job")
            print(f"Debug: {debug_file}")

        except Exception:
            pass

        return 0, 0, []

    # GET CARDS
    cards, card_count = (await get_job_cards(page))

    if card_count == 0:
        print("Không có job trên trang.")
        return 0, 0, []

    # PARSE
    jobs = []
    seen_ids = set()

    for i in range(card_count):
        card = cards.nth(i)
        job = await parse_job_card(card, category)

        if not job:
            continue

        job_id = job["job_id"]

        if not job_id:
            continue

        if job_id in seen_ids:
            continue

        seen_ids.add(job_id)
        jobs.append(job)

    return (card_count, len(jobs), jobs)

async def main():
    # TIMESTAMP
    crawled_at = (
        datetime.now()
        .astimezone()
        .isoformat()
    )

    # PLAYWRIGHT
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="vi-VN",

            viewport={
                "width": 1920,
                "height": 1080
            },

            user_agent=(
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/150.0.0.0 "
                "Safari/537.36"
            )
        )

        context.set_default_timeout(PAGE_TIMEOUT)
        listing_page = (await context.new_page())

        all_basic_jobs = []
        global_seen_ids = set()
        category_statistics = {}

        # CRAWL CATEGORY
        for category in CATEGORIES:
            category_name = (category["category_name"])

            print()
            print(category_name)

            first_page_url = build_page_url(category, 1)

            success = False

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await listing_page.goto(
                        first_page_url,
                        wait_until="domcontentloaded",
                        timeout=PAGE_TIMEOUT
                    )

                    await listing_page.wait_for_timeout(1500)

                    await listing_page.locator("div.item_vl[data-newid]"
                    ).first.wait_for(
                        state="attached",
                        timeout=30000
                    )

                    success = True
                    break

                except PlaywrightTimeoutError:
                    print(
                        f"Timeout page 1 "
                        f"{attempt}/{MAX_RETRIES}"
                    )

                except Exception as e:
                    print(
                        f"Error page 1 "
                        f"{attempt}/{MAX_RETRIES}: "
                        f"{e}"
                    )

            if not success:
                print(
                    f"[ERROR] Không mở được category "
                    f"'{category_name}'"
                )

                category_statistics[
                    category_name
                ] = {
                    "last_page": 0,
                    "pages_crawled": 0,
                    "jobs": 0
                }

                continue

            last_page = await get_last_page(listing_page)

            print()
            print(
                f"Category: "
                f"{category_name}"
            )

            print(
                f"Last page: "
                f"{last_page}"
            )

            # 3. CRAWL PAGE 1 → LAST PAGE
            category_jobs = 0
            pages_crawled = 0

            for page_number in range(1, last_page + 1):
                (
                    card_count,
                    job_count,
                    page_jobs
                ) = await crawl_category_page(
                    listing_page,
                    category,
                    page_number
                )

                # PAGE EMPTY
                if job_count == 0:
                    print(
                        f"[ERROR] Page "
                        f"{page_number} "
                        f"không có job."
                    )
                    continue

                pages_crawled += 1
                new_jobs = 0

                for job in page_jobs:

                    job_id = job["job_id"]

                    if (job_id in global_seen_ids):
                        continue

                    global_seen_ids.add(job_id)

                    all_basic_jobs.append(job)

                    category_jobs += 1
                    new_jobs += 1

                print(f"Page {page_number}: {new_jobs} jobs mới")

        print(
            "-" * 100
        )

        print(
            f"Tổng job unique: "
            f"{len(all_basic_jobs)}"
        )

        # CRAWL DETAIL
        print()
        print(
            "BẮT ĐẦU CRAWL DETAIL"
        )

        results = []

        total_jobs = len(
            all_basic_jobs
        )

        for index, job_basic in enumerate(
            all_basic_jobs,
            start=1
        ):

            print()
            print(
                f"DETAIL "
                f"[{index}/{total_jobs}]"
            )

            result = await crawl_job_detail(
                context=context,
                job_basic=job_basic,
                crawled_at=crawled_at
            )

            if result:
                results.append(result)

        # SAVE JSON
        with open(
            OUTPUT_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                results,
                f,
                ensure_ascii=False,
                indent=4
            )

        # FINAL
        print()
        print("#" * 100)

        print(
            "CRAWL HOÀN THÀNH"
        )

        print(
            "#" * 100
        )

        print(
            f"Danh mục: "
            f"{len(CATEGORIES)}"
        )

        print(
            f"Job unique: "
            f"{len(all_basic_jobs)}"
        )

        print(
            f"Detail thành công: "
            f"{len(results)}"
        )

        print(f"Output: {OUTPUT_FILE}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())