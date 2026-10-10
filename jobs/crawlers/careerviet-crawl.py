import asyncio
import json
import re
import math
from pathlib import Path
from datetime import datetime

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# CẤU HÌNH CHUNG
BASE_DOMAIN = "https://careerviet.vn"

OUTPUT_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "jobs"
    / f"careerviet_all_jobs_raw_"
      f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
)

# DANH SÁCH DANH MỤC CẦN CRAWL: 10
CATEGORIES = [
    {
        "category_name": "Kế toán - Kiểm toán",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "ke-toan-kiem-toan-c2-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "ke-toan-kiem-toan-c2-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Bán lẻ - Bán sỉ",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "ban-le-ban-si-c30-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "ban-le-ban-si-c30-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Nhân sự",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "nhan-su-c22-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "nhan-su-c22-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Thực phẩm - Đồ uống",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "thuc-pham-do-uong-c21-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "thuc-pham-do-uong-c21-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Business Analyst",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "business-analyst-k-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "business-analyst-k-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Tài chính - Đầu tư",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "tai-chinh-dau-tu-c59-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "tai-chinh-dau-tu-c59-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Ngân hàng",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "ngan-hang-c19-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "ngan-hang-c19-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Dịch vụ khách hàng",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "dich-vu-khach-hang-c12-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "dich-vu-khach-hang-c12-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Xây dựng",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "xay-dung-c8-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "xay-dung-c8-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Sản xuất - Vận hành sản xuất",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "san-xuat-van-hanh-san-xuat-c25-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "san-xuat-van-hanh-san-xuat-c25-trang-{page}-vi.html"
        )
    },

    {
        "category_name": "Quản lý - Điều hành",
        "base_url": (
            "https://careerviet.vn/viec-lam/"
            "quan-ly-dieu-hanh-c17-vi.html"
        ),
        "page_url_template": (
            "https://careerviet.vn/viec-lam/"
            "quan-ly-dieu-hanh-c17-trang-{page}-vi.html"
        )
    },
]

# TẠO URL
def build_page_url(category, page_number):
    if page_number == 1:
        return category["base_url"]

    return category["page_url_template"].format(page=page_number)

# LẤY TEXT AN TOÀN
async def get_text(locator):
    try:
        if await locator.count() == 0:
            return None

        text = await locator.first.inner_text()

        return text.strip() if text else None

    except Exception:
        return None

# LẤY TỔNG SỐ JOB ĐỂ XÁC ĐỊNH SỐ TRANG CẦN CRAWLS
async def get_total_jobs(page):
    amount_element = page.locator("div.job-found-amout h1").first

    if await amount_element.count() == 0:
        print("[ERROR] Không tìm thấy tổng số việc làm")
        return None

    text = (await amount_element.inner_text()).strip()
    match = re.search(r"[\d,.]+", text)

    if not match:
        print("[ERROR] Không tìm thấy số lượng job")
        return None

    number_text = match.group()

    total_jobs = int(number_text
        .replace(",", "")
        .replace(".", "")
    )

    return total_jobs

# LẤY SECTION DETAIL THEO TITLE
async def get_detail_section(detail_page, title_text):
    sections = detail_page.locator("div.detail-row")
    count = await sections.count()

    for i in range(count):
        section = sections.nth(i)
        title = section.locator("h2.detail-title").first

        if await title.count() == 0:
            continue

        title_value = (await title.inner_text()).strip()

        # Chuẩn hóa khoảng trắng
        title_value = re.sub(r"\s+", " ", title_value)

        if title_text.lower() in title_value.lower():
            return section

    return None

# CRAWL JOB DETAIL
async def crawl_job_detail(detail_page, job_url):
    detail_data = {
        "experience": None,
        "education": None,
        "skills": [],
        "description": None,
        "requirements": None
    }

    if not job_url:
        return detail_data

    try:
        # Chờ render
        await detail_page.goto(
            job_url,
            wait_until="domcontentloaded",
            timeout=30000
        )

        # KINH NGHIỆM
        info_items = detail_page.locator(".content_fck li")
        info_count = await info_items.count()

        for i in range(info_count):
            text = (await info_items.nth(i).inner_text()).strip()

            if "Kinh nghiệm" in text:
                parts = text.split(":", 1)

                if len(parts) == 2:
                    detail_data["experience"] = (parts[1].strip())

                else:
                    detail_data["experience"] = text

                break

        # BẰNG CẤP
        for i in range(info_count):
            text = (await info_items.nth(i).inner_text()).strip()

            if "Bằng cấp" in text:
                parts = text.split(":", 1)

                if len(parts) == 2:
                    detail_data["education"] = (parts[1].strip())

                else:
                    detail_data["education"] = text
                
                break

        # SKILLS
        skill_elements = detail_page.locator(".job-tags ul li a")
        skill_count = await skill_elements.count()
        skills = []

        for i in range(skill_count):
            skill = (await skill_elements
                .nth(i)
                .inner_text()
            ).strip()

            if skill:
                skills.append(skill)

        # Loại duplicate nhưng giữ nguyên thứ tự
        detail_data["skills"] = list(dict.fromkeys(skills))

        # MÔ TẢ CÔNG VIỆC
        description_section = (await get_detail_section(detail_page, "Mô tả Công việc"))

        if description_section:
            content = description_section.locator(":scope > div")
            content_count = await content.count()

            if content_count > 0:
                # Lấy div cuối
                description = (await content.nth(content_count - 1)
                    .inner_text()
                ).strip()

                if description:
                    detail_data["description"] = (description)

        # YÊU CẦU CÔNG VIỆC
        requirements_section = (await get_detail_section(detail_page, "Yêu Cầu Công Việc"))

        if requirements_section:
            content = requirements_section.locator(":scope > div")
            content_count = await content.count()

            if content_count > 0:
                requirements = (await content.nth(content_count - 1)
                    .inner_text()
                ).strip()

                if requirements:
                    detail_data["requirements"] = (requirements)

        return detail_data

    except Exception as e:
        print(f"[DETAIL ERROR] {job_url} : {type(e).__name__}: {e}")
        return detail_data

# CRAWL MỘT JOB 
async def crawl_job_with_page(card, detail_page, category_name, crawled_at):
    try:
        # JOB LINK
        job_link = card.locator("h2 a.job_link").first

        if await job_link.count() == 0:
            print("[ERROR] Không tìm thấy job link")
            return None

        # JOB ID
        job_id = await job_link.get_attribute("data-id")

        # JOB TITLE
        job_title = await get_text(job_link)

        # JOB URL
        job_url = await job_link.get_attribute("href")

        if (job_url and job_url.startswith("/")):
            job_url = (BASE_DOMAIN + job_url)

        # COMPANY
        company = await get_text(card.locator(".company-name"))

        # SALARY
        salary = await get_text(card.locator(".salary"))

        # LOCATION
        location = await get_text(card.locator(".location"))

        # TIME
        times = card.locator(".time time")
        time_count = await times.count()

        expiration_date = None
        posted_date = None

        if time_count >= 1:
            expiration_date = await get_text(times.nth(0))

        if time_count >= 2:
            posted_date = await get_text(times.nth(1))

        # CRAWL DETAIL PAGE
        detail_data = await crawl_job_detail(
            detail_page=detail_page,
            job_url=job_url
        )

        job = {
            "category_name": category_name,
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "salary": salary,
            "location": location,
            "expiration_date": expiration_date,
            "posted_date": posted_date,
            "job_url": job_url,
            "experience": (detail_data["experience"]),
            "education": (detail_data["education"]),
            "skills": (detail_data["skills"]),
            "description": (detail_data["description"]),
            "requirements": (detail_data["requirements"]),
            "source_name": "CareerViet",
            "crawled_at": crawled_at
        }

        return job

    except Exception as e:
        print("[ERROR] Crawl job:", e)
        return None

async def crawl_job(card, context, semaphore, category_name, crawled_at):
    async with semaphore:
        detail_page = await context.new_page()

        try:
            job = await crawl_job_with_page(
                card=card,
                detail_page=detail_page,
                category_name=category_name,
                crawled_at=crawled_at
            )
            return job

        finally:
            await detail_page.close()

# CRAWL MỘT DANH MỤC
async def crawl_category(listing_page, context, semaphore, category, crawled_at, seen_job_ids):
    category_name = category["category_name"]
    print(category_name)

    first_url = build_page_url(category, 1)
    print("Opening:", first_url)

    try:
        await listing_page.goto(
            first_url,
            wait_until="domcontentloaded",
            timeout=60000
        )

        print("Final URL:", listing_page.url)

    except Exception as e:
        print(f"[ERROR] Cannot open {category_name}: {e}")
        return []

    # CHỜ TRANG RENDER
    try:
        await listing_page.locator(
            "div.jobs-side-list div.job-item"
        ).first.wait_for(
            state="visible",
            timeout=15000
        )
    except PlaywrightTimeoutError:
        print("[ERROR] Danh sách job chưa xuất hiện")
        return []

    # LẤY TỔNG SỐ JOB
    total_jobs = await get_total_jobs(listing_page)

    if total_jobs is None:
        print(f"[ERROR] Không lấy được total jobs của {category_name}")
        return []

    # ĐẾM JOB
    jobs = listing_page.locator("div.jobs-side-list div.job-item")
    jobs_per_page = await jobs.count()

    if jobs_per_page == 0:
        print(f"[ERROR] Không tìm thấy job trong {category_name}")
        return []

    # TÍNH SỐ TRANG
    total_pages = math.ceil(total_jobs / jobs_per_page)
    print("Category      :", category_name)
    print("Total jobs    :", total_jobs)
    print("Total pages   :", total_pages)

    # DANH SÁCH JOB CỦA CATEGORY
    category_jobs = []

    # CRAWL TỪNG TRANG
    for page_number in range(1, total_pages + 1):
        if page_number > 1:
            url = build_page_url(category, page_number)

            try:
                await listing_page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=60000
                )

            except Exception as e:
                print(f"[ERROR] Cannot open page {page_number}: {e}")
                break

            # Chờ render
            try:
                await listing_page.locator(
                    "div.jobs-side-list div.job-item"
                ).first.wait_for(
                    state="visible",
                    timeout=15000
                )
            except PlaywrightTimeoutError:
                print("[ERROR] Danh sách job chưa xuất hiện")
                return []

        # LẤY JOB CARDS
        jobs = listing_page.locator("div.jobs-side-list div.job-item")
        count = await jobs.count()

        # KHÔNG CÓ JOB
        if count == 0:
            print(f"[ERROR] Không có job trong {category_name}")
            break

        if count == 0:
            print(f"[ERROR] Không có job trong {category_name}")
            break

        # Tạo các tác vụ crawl chi tiết
        tasks = []

        for i in range(count):
            card = jobs.nth(i)

            job_link = card.locator("h2 a.job_link").first

            if await job_link.count() == 0:
                continue

            job_id = await job_link.get_attribute("data-id")

            # Chống trùng trước khi tạo tác vụ
            if job_id and job_id in seen_job_ids:
                continue

            if job_id:
                seen_job_ids.add(job_id)

            tasks.append(
                asyncio.create_task(
                    crawl_job(
                        card=card,
                        context=context,
                        semaphore=semaphore,
                        category_name=category_name,
                        crawled_at=crawled_at
                    )
                )
            )

        # Chờ các tác vụ của trang này hoàn tất
        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

        page_jobs = 0

        for result in results:
            if isinstance(result, Exception):
                print(f"[ERROR] Task thất bại: {result}")
                continue

            if result is None:
                continue

            category_jobs.append(result)
            page_jobs += 1

            print(
                f"[{category_name}] "
                f"Page {page_number} "
                f"-> {result['job_title']}"
            )

        print(f"Page {page_number} completed.")
        print("New jobs:", page_jobs)
        print("Job total:", len(category_jobs))

    return category_jobs

async def main():
    async with async_playwright() as p:
        # BROWSER
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            locale="vi-VN",
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            )
        )

        # Số trang chi tiết tối đa được xử lý đồng thời
        CONCURRENCY = 10

        semaphore = asyncio.Semaphore(CONCURRENCY)

        # Page chuyên dùng cho danh sách
        listing_page = await context.new_page()

        # LẤY TIME
        crawled_at = (
            datetime.now()
            .astimezone()
            .isoformat(
                timespec="seconds"
            )
        )

        print("Start time:", crawled_at)
        print("Categories:", len(CATEGORIES))

        # TẤT CẢ JOB
        all_jobs = []

        # SET CHỐNG TRÙNG
        seen_job_ids = set()

        # CRAWL TỪNG CATEGORY
        for index, category in enumerate(CATEGORIES, start=1):
            print(f"CATEGORY {index}/{len(CATEGORIES)}")

            # CRAWL CATEGORY
            category_jobs = await crawl_category(
                listing_page=listing_page,
                context=context,
                semaphore=semaphore,
                category=category,
                crawled_at=crawled_at,
                seen_job_ids=seen_job_ids
            )

            # ADD VÀO DATASET CHUNG
            all_jobs.extend(category_jobs)
            print("Total jobs collected:", len(all_jobs))

        # LƯU JSON
        print("SAVING DATA")

        with open(
            OUTPUT_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                all_jobs,
                f,
                ensure_ascii=False,
                indent=4
            )

        print("ALL CATEGORIES COMPLETED")
        print("Categories:", len(CATEGORIES)) 
        print("Total jobs:", len(all_jobs))
        print("Output:", OUTPUT_FILE)
        print("Crawled at:", crawled_at)

        await browser.close()

asyncio.run(main())