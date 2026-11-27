<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# توسعه بدون هوش مصنوعی

## همه چیز را آماده کنید <a id="prepare-everything"></a>

اگر هنوز این کار را نکرده‌اید:

[نرم‌افزارهای لازم را نصب کنید](./install-guide.md)

[یک فضای کاری (workspace) برای gg راه‌اندازی کنید](./install-guide/install-gg-workspace-guide.md)

## به فضای کاری خود بروید <a id="switch-into-your-workspace"></a>

```bash
cd ~/dev/
```

## مخزن‌ها را به تیکت اضافه کنید <a id="add-repos-to-the-ticket"></a>

```bash
gg do add repo1 repo2
```

همه مخزن‌ها (repos) را در یک فراخوانی نام ببرید: هر فراخوانی ocean را
به‌روز می‌کند و گراف وابستگی‌ها را دوباره می‌سازد.

## فضای کاری را در VS Code باز کنید <a id="open-the-workspace-in-vscode"></a>

```bash
gg do code
```

## پیاده‌سازی <a id="implement"></a>

قابلیت‌ها (features) را بر اساس راهنماها پیاده‌سازی کنید.

## کامیت (Commit) <a id="commit"></a>

```bash
gg do commit
```

## پوش (Push) <a id="push"></a>

```bash
gg do push
```

## بازبینی (Review) <a id="review"></a>

```bash
gg do review
```

## انتشار (Publish) <a id="publish"></a>

از ریشه فضای کاری منتشر کنید و تیکت را با `-i` نام ببرید:

```bash
cd ~/dev/
gg do publish -i <ticket>
```

در پایان gg پوشه تیکت را به سطل زباله منتقل می‌کند. در ویندوز تا وقتی
یک ترمینال در پوشه تیکت باز است، این کار شکست می‌خورد
("used by another process").
