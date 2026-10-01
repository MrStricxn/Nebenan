import asyncio
from playwright.async_api import async_playwright

async def save_cookies():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto("https://nebenan.de/login")
        print("\nЗалогинься в открывшемся браузере, затем нажми Enter здесь...")
        input()
        await ctx.storage_state(path="cookies/account1.json")
        print("Куки сохранены в cookies/account1.json")
        await browser.close()

asyncio.run(save_cookies())
