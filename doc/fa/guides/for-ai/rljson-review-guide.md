<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# راهنمای بازبینی rljson

یک تغییر در مخزن‌های rljson چگونه بازبینی (review) می‌شود، علاوه بر راهنمای
بازبینی. زود شکست بخورید (fail fast): بررسی‌های ارزان اول، نخستین یافته
بازبینی را پایان می‌دهد، و با قبولیِ ناقص هیچ چیزی ادغام (merge) نمی‌شود.

## آن را کامل بخوانید <a id="read-it-whole"></a>

- diff کامل را در برابر `main` فعلی بخوانید، نه توضیحات را و نه `main`ی که
  شاخه از آن جدا شده است.
- بررسی کنید که شاخه نه از `main` عقب است و نه تعارض (conflict) دارد: یک
  شاخهٔ کهنه ممکن است چیزی را اصلاح کند که `main` پیش‌تر اصلاح کرده است.
- یک موضوع در هر شاخه: یک تغییر نامرتبط یک یافته است.

## وابستگی‌ها را بررسی کنید <a id="check-the-dependencies"></a>

- هر وابستگی `@rljson/*` دقیقاً یک نسخهٔ منتشرشده است
  (`pnpm view @rljson/<pkg> version`).
- هیچ `pnpm.overrides` در `package.json` نباشد؛ هیچ مشخص‌کنندهٔ (specifier)
  `link:`، `file:` یا `workspace:`، هیچ `resolutions` و هیچ `node_modules`
  کامیت‌شده‌ای نباشد.
- `pnpm install --frozen-lockfile` با موفقیت اجرا شود.

## هیچ چیز در وابستگی وصله نشده باشد <a id="nothing-patched-into-a-dependency"></a>

- هیچ `patchedDependencies`، هیچ `patches/`، هیچ مرحلهٔ postinstall که در
  `node_modules` بنویسد، و هیچ `dist/` تحت کنترل نسخه نباشد.

## اصلاح در بسته‌ای است که به آن تعلق دارد <a id="the-fix-lives-in-the-package-it-belongs-to"></a>

- دربارهٔ هر فایل تغییرکرده بپرسید که آیا منطق آن به یک بستهٔ `@rljson/*`
  تعلق دارد. اگر چنین است، تغییر در همان بسته است، همان‌جا تست و پوشش داده
  شده، و آنچه این‌جا می‌ماند فقط یک نسخه و سیم‌کشی (wiring) است.
- راه‌حل موقت (workaround) سمت اپ برای نقص یک بسته یک یافته است، حتی اگر کار
  کند.

## از یک وضعیت تمیز بیلد کنید <a id="build-from-a-clean-state"></a>

- یک شاخهٔ معمولی: `rm -rf node_modules && pnpm install --frozen-lockfile
&& pnpm run build` در یک کلون تازه.
- یک تیکت gg: در تیکت، پس از `gg do import ticket` و `gg do localize` —
  شاخه‌های آن به یکدیگر لینک می‌شوند و به‌تنهایی نصب نمی‌شوند.
- `git status --short` پس از آن خالی می‌ماند.

## تست‌ها تغییر را ثابت می‌کنند <a id="tests-prove-the-change"></a>

- `pnpm test` سبز، ۱۰۰ درصد در هر فایل لمس‌شده.
- تست جدید بدون تغییر شکست می‌خورد: کد منبع را برگردانید، اجرا کنید،
  بازگردانید.

## خودِ خطوط <a id="the-lines-themselves"></a>

- هیچ تکراری از اصلاحی که `main` یا انتشار یک بسته از قبل دارد نباشد: نشانهٔ
  مشکل را جست‌وجو کنید.
- هیچ تابع کمکی (helper) بازنویسی‌شده، هیچ باقی‌ماندهٔ دیباگ، هیچ endpoint
  هاردکدشده، هیچ اعتبارنامه، کلید یا فایل `.env` نباشد.
- یک مدخل `CHANGELOG.md`، مستندات مطابق راهنمای rljson، `scripts` دست‌نخورده،
  سرآیند مجوز (license header) در فایل‌های جدید.

## گزارش <a id="report"></a>

- یافته‌ها به‌صورت کامنت pull request به نویسنده برگردانده می‌شوند. هرگز
  اصلاحات را در شاخهٔ شخص دیگری push نکنید، مگر آن‌که مالک (owner) بخواهد.
- به مالک با کلماتی کوتاه و ساده بگویید چه چیزی و کجا اشتباه است.
