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

# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://timviec365.vn"

SOURCE = "timviec365"

# ============================================================
# DANH SÁCH DANH MỤC
# ============================================================

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


# ============================================================
# CRAWL SETTINGS
# ============================================================

PAGE_TIMEOUT = 60_000

MAX_RETRIES = 3

MAX_JOBS_PER_PAGE = None

# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(text):
    """
    Chuẩn hóa text một dòng.
    """

    if not text:
        return ""

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


def clean_multiline_text(text):
    """
    Chuẩn hóa text nhiều dòng.
    """

    if not text:
        return ""

    lines = []

    for line in text.splitlines():
        line = re.sub(
            r"[ \t]+",
            " ",
            line
        ).strip()

        if line:
            lines.append(line)

    return "\n".join(lines)


async def get_text(
    locator,
    multiline=False
):
    """
    Lấy text an toàn.
    """

    try:

        if await locator.count() == 0:
            return ""

        if multiline:

            text = await locator.first.text_content()

            return clean_multiline_text(
                text
            )

        text = await locator.first.inner_text()

        return clean_text(
            text
        )

    except Exception:

        return ""


# ============================================================
# BUILD PAGE URL
# ============================================================

def build_page_url(category,page_number):
    """
    Tạo URL pagination.

    Page 1:
        base_url

    Page 2+:
        page_url_template
    """

    if page_number == 1:
        return category["base_url"]

    return category["page_url_template"].format(page=page_number)

# ============================================================
# GET LAST PAGE
# ============================================================

async def get_last_page(page) -> int:
    try:
        pagination_links = page.locator(
            "li.pagi_pre_all a.link_page"
        )

        count = await pagination_links.count()

        print(
            f"Pagination links: {count}"
        )

        # Không có pagination
        if count == 0:

            print(
                "Không có pagination."
            )

            print(
                "last_page = 1"
            )

            return 1

        page_numbers = set()

        # ====================================================
        # DUYỆT CÁC LINK
        # ====================================================

        for i in range(count):

            link = pagination_links.nth(i)

            # ------------------------------------------------
            # TEXT
            # ------------------------------------------------

            try:

                text = (
                    await link.inner_text()
                ).strip()

            except Exception:

                text = ""

            if text.isdigit():

                page_numbers.add(
                    int(text)
                )

            # ------------------------------------------------
            # HREF
            # ------------------------------------------------

            try:

                href = await link.get_attribute(
                    "href"
                )

            except Exception:

                href = None

            if href:

                match = re.search(
                    r"[?&]page=(\d+)",
                    href
                )

                if match:

                    page_numbers.add(
                        int(
                            match.group(1)
                        )
                    )

        # ====================================================
        # KHÔNG TÌM THẤY
        # ====================================================

        if not page_numbers:

            print(
                "Không tìm thấy số trang."
            )

            return 1

        # ====================================================
        # TRANG CUỐI
        # ====================================================

        last_page = max(
            page_numbers
        )

        print(f"Trang cuối: {last_page}")
        return last_page

    except Exception as e:
        print(f"get_last_page error: {e}")
        return 1


# ============================================================
# GET JOB CARDS
# ============================================================

async def get_job_cards(page):
    selector = (
        "div.item_vl[data-newid]"
    )

    cards = page.locator(
        selector
    )

    count = await cards.count()

    print(
        f"Số job hiển thị: {count}"
    )

    return cards, count


# ============================================================
# PARSE JOB CARD
# ============================================================

async def parse_job_card(
    card,
    category,
    page_number
):
    """
    Lấy thông tin cơ bản từ listing.
    """

    try:

        # ====================================================
        # JOB ID
        # ====================================================

        job_id = await card.get_attribute(
            "data-newid"
        )

        if not job_id:
            return None

        # ====================================================
        # TITLE
        # ====================================================

        title_locator = card.locator("h2.box_title_new a.title_new")
        job_title = await get_text(title_locator)

        # ====================================================
        # URL
        # ====================================================

        href = await title_locator.get_attribute("href")

        if not href:
            return None

        job_url = urljoin(BASE_URL, href)

        # ====================================================
        # COMPANY
        # ====================================================

        company = await get_text(card.locator("a.name_com"))

        # ====================================================
        # LOCATION
        # ====================================================

        location = await get_text(
            card.locator(
                ".job_city"
            )
        )

        # ====================================================
        # SALARY
        # ====================================================

        salary = await get_text(
            card.locator(
                ".job_money"
            )
        )

        # ====================================================
        # EXPERIENCE
        # ====================================================

        experience = await get_text(
            card.locator(
                ".item_catenew_exp"
            )
        )

        # ====================================================
        # DEADLINE
        # ====================================================

        deadline = await get_text(
            card.locator(
                ".job_time"
            )
        )

        # ====================================================
        # RETURN
        # ====================================================

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

            "_page": page_number,
        }

    except Exception as e:
        print(f"Parse card error: {e}")
        return None

# ============================================================
# DETAIL: LẤY THÔNG TIN THEO TITLE
# ============================================================

async def get_detail_value(
    page,
    title
):
    try:

        items = page.locator(
            "div.itemDetailInfo_center"
        )

        count = await items.count()

        for i in range(count):

            item = items.nth(i)

            title_text = await get_text(
                item.locator(
                    ".titleContentSalary"
                )
            )

            if (
                title_text.lower()
                ==
                title.lower()
            ):

                return await get_text(
                    item.locator(
                        ".valContentSalary"
                    )
                )

    except Exception:

        pass

    return ""


# ============================================================
# DETAIL: SECTION
# ============================================================

async def get_detail_section(
    page,
    section_title
):
    """
        Mô tả công việc
        Yêu cầu
    """

    try:
        sections = page.locator("div.itemInfoSpecific")
        count = await sections.count()

        for i in range(count):
            section = sections.nth(i)

            title = await get_text(
                section.locator(
                    "h2.titleInfoSpecific"
                )
            )

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


# ============================================================
# DETAIL: JOB INFORMATION
# ============================================================

async def get_job_information(
    page
):
    """
        Bằng cấp
        Cập nhật
    """

    result = {}

    try:

        items = page.locator(
            "div.itemYauCauKhac"
        )

        count = await items.count()

        for i in range(count):

            item = items.nth(i)

            title = await get_text(
                item.locator(
                    ".titleYauCauKhac"
                )
            )

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

# ============================================================
# CRAWL DETAIL
# ============================================================

async def crawl_job_detail(context, job_basic, crawled_at):
    job_url = job_basic["job_url"]

    print()
    print("-" * 80)

    print(
        f"{job_basic['job_id']}"
    )

    print(
        f"{job_basic['job_title']}"
    )

    print(f"{job_url}")

    page = await context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT)
    success = False

    # ========================================================
    # RETRY
    # ========================================================

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(
                job_url,
                wait_until="domcontentloaded",
                timeout=PAGE_TIMEOUT
            )

            await page.wait_for_timeout(1000)

            # ------------------------------------------------
            # CHECK DETAIL
            # ------------------------------------------------

            if (await page.locator("h1.titleNew").count() == 0):
                print(
                    f"Không thấy detail "
                    f"{attempt}/{MAX_RETRIES}"
                )

                continue

            success = True
            break

        except PlaywrightTimeoutError:
            print(
                f"Timeout "
                f"{attempt}/{MAX_RETRIES}"
            )

        except Exception as e:
            print(
                f"Error "
                f"{attempt}/{MAX_RETRIES}: "
                f"{e}"
            )

    # ========================================================
    # DETAIL FAILED
    # ========================================================

    if not success:

        await page.close()

        return None

    try:

        # ====================================================
        # TITLE
        # ====================================================

        job_title = await get_text(
            page.locator(
                "h1.titleNew"
            )
        )

        if not job_title:

            job_title = job_basic[
                "job_title"
            ]

        # ====================================================
        # SALARY
        # ====================================================

        salary = await get_detail_value(
            page,
            "Mức lương"
        )

        if not salary:

            salary = job_basic[
                "salary"
            ]

        # ====================================================
        # LOCATION
        # ====================================================

        location = await get_detail_value(
            page,
            "Địa điểm"
        )

        if not location:
            location = job_basic["location"]

        # ====================================================
        # DEADLINE
        # ====================================================

        deadline = await get_detail_value(page, "Hạn nộp")

        if not deadline:
            deadline = job_basic["deadline"]

        # ====================================================
        # EXPERIENCE
        # ====================================================

        experience = await get_detail_value(
            page,
            "Kinh nghiệm"
        )

        if not experience:

            experience = job_basic[
                "experience"
            ]

        # ====================================================
        # DESCRIPTION
        # ====================================================

        description = await get_detail_section(
            page,
            "Mô tả công việc"
        )

        # ====================================================
        # REQUIREMENTS
        # ====================================================

        requirements = await get_detail_section(
            page,
            "Yêu cầu"
        )

        # ====================================================
        # JOB INFORMATION
        # ====================================================

        job_information = (
            await get_job_information(
                page
            )
        )

        # ====================================================
        # SKILLS
         # ====================================================

        skills = []

        # ====================================================
        # FINAL RECORD
        # ====================================================

        job = {

            # ------------------------------------------------
            # IDENTIFICATION
            # ------------------------------------------------

            "category":
                job_basic[
                    "category_name"
                ],

            "job_id":
                job_basic[
                    "job_id"
                ],

            "job_title": job_title,

            "company": job_basic["company"],

            # ------------------------------------------------
            # BASIC
            # ------------------------------------------------

            "salary": salary,

            "location": location,

            "deadline": deadline,

            "posted_at": job_information.get("posted_at", ""),

            "job_url": job_url,

            "experience": experience,

            "education": job_information.get("education", ""),

            # ------------------------------------------------
            # DETAIL
            # ------------------------------------------------

            "skills":
                skills,

            "description":
                description,

            "requirements":
                requirements,

            # ------------------------------------------------
            # METADATA
            # ------------------------------------------------

            "source":
                SOURCE,

            "crawled_at":
                crawled_at,
        }

        print(
            f"SUCCESS: {job_title}"
        )

        return job

    except Exception as e:

        print(
            f"Detail error "
            f"{job_basic['job_id']}: {e}"
        )

        return None

    finally:
        await page.close()


# ============================================================
# CRAWL ONE CATEGORY PAGE
# ============================================================

async def crawl_category_page(
    page,
    category,
    page_number
):

    page_url = build_page_url(
        category,
        page_number
    )

    print()
    print("=" * 80)

    print(
        f"Danh mục: "
        f"{category['category_name']}"
    )

    print(
        f"Trang: "
        f"{page_number}"
    )

    print(
        f"URL: "
        f"{page_url}"
    )

    print("=" * 80)

    # ========================================================
    # OPEN PAGE
    # ========================================================

    success = False

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            await page.goto(
                page_url,
                wait_until="domcontentloaded",
                timeout=PAGE_TIMEOUT
            )

            await page.wait_for_timeout(
                1500
            )

            # ------------------------------------------------
            # CHỜ JOB CARD
            # ------------------------------------------------

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

    # ========================================================
    # FAILED
    # ========================================================

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

            print(
                f"Không lấy được job"
            )

            print(
                f"Debug: {debug_file}"
            )

        except Exception:
            pass

        return 0, 0, []

    # ========================================================
    # GET CARDS
    # ========================================================

    cards, card_count = (
        await get_job_cards(
            page
        )
    )

    if card_count == 0:
        print("Không có job trên trang.")

        return 0, 0, []

    # ========================================================
    # LIMIT TEST
    # ========================================================

    crawl_count = card_count

    if MAX_JOBS_PER_PAGE is not None:
        crawl_count = min(card_count,MAX_JOBS_PER_PAGE)

    # ========================================================
    # PARSE
    # ========================================================

    jobs = []
    seen_ids = set()

    for i in range(crawl_count):
        card = cards.nth(i)
        job = await parse_job_card(card, category, page_number)

        if not job:
            continue

        job_id = job["job_id"]

        if not job_id:
            continue

        if job_id in seen_ids:
            continue

        seen_ids.add(job_id)
        jobs.append(job)

    print()
    print(
        f"Page {page_number}: "
        f"{len(jobs)} job"
    )

    return (card_count, len(jobs), jobs)


# ============================================================
# MAIN
# ============================================================

async def main():

    # ========================================================
    # TIMESTAMP
    # ========================================================

    crawled_at = (
        datetime.now()
        .astimezone()
        .isoformat()
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_file = Path(
        f"timviec365_all_jobs_"
        f"{timestamp}.json"
    )

    # ========================================================
    # PLAYWRIGHT
    # ========================================================

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=True
        )

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

        context.set_default_timeout(
            PAGE_TIMEOUT
        )

        listing_page = (
            await context.new_page()
        )

        # ====================================================
        # GLOBAL
        # ====================================================

        all_basic_jobs = []

        global_seen_ids = set()

        category_statistics = {}

        # ====================================================
        # CRAWL CATEGORY
        # ====================================================

        for category in CATEGORIES:

            category_name = (
                category[
                    "category_name"
                ]
            )

            print()
            print()
            print(
                "#" * 100
            )

            print(
                f"{category_name}"
            )

            print(
                "#" * 100
            )

            # =================================================
            # 1. MỞ PAGE 1
            # =================================================

            first_page_url = build_page_url(
                category,
                1
            )

            print(
                f"Page 1: "
                f"{first_page_url}"
            )

            success = False

            for attempt in range(
                1,
                MAX_RETRIES + 1
            ):

                try:

                    await listing_page.goto(
                        first_page_url,
                        wait_until="domcontentloaded",
                        timeout=PAGE_TIMEOUT
                    )

                    await listing_page.wait_for_timeout(
                        1500
                    )

                    await listing_page.locator(
                        "div.item_vl[data-newid]"
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

            # =================================================
            # PAGE 1 FAILED
            # =================================================

            if not success:

                print(
                    f"Không mở được category "
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

            # =================================================
            # 2. LẤY TRANG CUỐI
            # =================================================

            last_page = await get_last_page(
                listing_page
            )

            print()
            print(
                f"Category: "
                f"{category_name}"
            )

            print(
                f"Last page: "
                f"{last_page}"
            )

            # =================================================
            # 3. CRAWL PAGE 1 → LAST PAGE
            # =================================================

            category_jobs = 0

            pages_crawled = 0

            for page_number in range(
                1,
                last_page + 1
            ):

                (
                    card_count,
                    job_count,
                    page_jobs
                ) = await crawl_category_page(
                    listing_page,
                    category,
                    page_number
                )

                # ------------------------------------------------
                # PAGE EMPTY
                # ------------------------------------------------

                if job_count == 0:

                    print(
                        f"Page "
                        f"{page_number} "
                        f"không có job."
                    )

                    continue

                pages_crawled += 1

                # ------------------------------------------------
                # GLOBAL DEDUP
                # ------------------------------------------------

                new_jobs = 0

                for job in page_jobs:

                    job_id = job[
                        "job_id"
                    ]

                    if (
                        job_id
                        in global_seen_ids
                    ):
                        continue

                    global_seen_ids.add(
                        job_id
                    )

                    all_basic_jobs.append(
                        job
                    )

                    category_jobs += 1

                    new_jobs += 1

                print(
                    f"Job mới: "
                    f"{new_jobs}"
                )

        print(
            "-" * 100
        )

        print(
            f"Tổng job unique: "
            f"{len(all_basic_jobs)}"
        )

        # ========================================================
        # CRAWL DETAIL
        # ========================================================

        print()
        print(
            "#" * 100
        )

        print(
            "BẮT ĐẦU CRAWL DETAIL"
        )

        print(
            "#" * 100
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

                results.append(
                    result
                )

        # ========================================================
        # SAVE JSON
        # ========================================================

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                results,
                f,
                ensure_ascii=False,
                indent=4
            )

        # ========================================================
        # FINAL
        # ========================================================

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

        print(
            f"Detail thất bại: "
            f"{len(all_basic_jobs) - len(results)}"
        )

        print(f"Output: {output_file}")
        print("#" * 100)

        await browser.close()

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())