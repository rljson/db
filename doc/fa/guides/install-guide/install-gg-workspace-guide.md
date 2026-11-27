<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# ساختن یک فضای کاری gg

## یک پوشه توسعه بسازید <a id="create-a-dev-dir"></a>

```bash
mkdir -p ~/dev2
cd ~/dev2
```

## یک فضای کاری gg راه‌اندازی کنید <a id="init-a-gg-workspace"></a>

```bash
gg do init workspace
```

این فرمان پوشه `.ocean` را می‌سازد و جدیدترین `dna_gg` را در پوشه فضای
کاری نمونه‌سازی می‌کند: راهنماهای gg، مهارت‌ها (skills) و بلوک مدیریت‌شده
`CLAUDE.md`. همان کاری را می‌کند که `gg dna init` و
`gg dna add dna_gg --workspace` به‌صورت دستی انجام می‌دهند — `add` خودش
DNA را می‌سازد و `gg dna build` لازم نیست — اما فایل‌های pub آن‌ها را
بیرون از پوشه فضای کاری نگه می‌دارد. هر تیکت یک نسخه از مهارت‌ها دریافت
می‌کند.

## مخزن‌های خود را اضافه کنید <a id="add-your-repositories"></a>

```bash
gg do add dnaOrgUrl
```
