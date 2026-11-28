<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# نصب gg

`gg` ابزار خط فرمان (CLI) یکپارچه Dart برای توسعه در rljson است.
بررسی‌های پیش از کامیت (pre-commit) را در یک مخزن اجرا می‌کند و کامیت،
پوش، بازبینی و انتشار را در همه مخزن‌های یک تیکت هماهنگ می‌کند.

## پیش‌نیازها <a id="prerequisites"></a>

ابتدا [Flutter](./install-flutter-guide.md) را نصب کنید. فرمان `dart` که
در ادامه استفاده می‌شود از آن می‌آید.

```bash
dart --version
```

## نصب gg <a id="install-gg"></a>

```bash
dart pub global activate gg
```

## افزودن pub cache به PATH <a id="add-the-pub-cache-to-your-path"></a>

برنامه‌های Dart که به‌صورت سراسری فعال می‌شوند در pub cache نصب می‌شوند.
پوشه `bin` آن را به `PATH` اضافه کنید، وگرنه فرمان `gg` پیدا نمی‌شود.

مک و لینوکس:

```bash
echo 'export PATH="$HOME/.pub-cache/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

در ویندوز، پوشه زیر را به متغیر محیطی `Path` اضافه کنید:

```text
%LOCALAPPDATA%\Pub\Cache\bin
```

سپس ترمینال را دوباره اجرا کنید.

## بررسی نصب <a id="verify-installation"></a>

```bash
gg --help
```

## به‌روزرسانی gg <a id="update-gg"></a>

برای نصب جدیدترین نسخه، فعال‌سازی را دوباره اجرا کنید:

```bash
dart pub global activate gg
```

## استفاده <a id="usage"></a>

`gg` تیکت‌محور است: آن را درون یک تیکت اجرا کنید تا هر عمل روی همه
مخزن‌های آن تیکت اعمال شود.

```bash
gg                # the command groups: do, can, did
gg do -h          # everything you can act on
gg can commit     # check before committing
```

همچنین [راهنمای توسعه](../develop-guide.md) را ببینید.
