<#
.SYNOPSIS
    Mở sẵn Windows Terminal chia 6 ô cho buổi demo: tracker, 2 seeder, 2 leecher và 1 ô công cụ.

.DESCRIPTION
    Mỗi ô đã đặt sẵn PEER_ID, PEER_PORT, UPLOAD_DELAY và hiện sẵn lệnh của node đó.
    Đến lượt node nào thì bấm vào ô đó và nhấn Enter. Muốn chạy lại thì gõ: go

    Mặc định script dọn dữ liệu của lần chạy trước (thư mục data/) và tắt các tiến trình
    python còn chiếm cổng 8000, 9001–9007, để mỗi lần demo đều bắt đầu sạch.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1
    powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1 -SizeMB 20 -Delay 0.5
    powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1 -Keep     # giữ dữ liệu cũ
#>
param(
    [int]$SizeMB = 40,          # kích thước file mẫu (MB); 40 MB = 160 chunk
    [double]$Delay = 1,         # giây/chunk khi upload, để người xem kịp theo dõi và kịp gây lỗi
    [switch]$Keep,              # không xóa dữ liệu của lần chạy trước
    [string]$Role = "",         # dùng nội bộ: vai trò của từng ô
    [string]$FileId = ""        # dùng nội bộ: file_id truyền cho các ô
)

$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$Root   = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$Src    = Join-Path $Root "src"
$Sample = Join-Path $Root "sample\test.bin"
$Self   = $PSCommandPath
$DemoPorts = @{ "peer-1" = 9001; "peer-2" = 9002; "peer-3" = 9003; "peer-4" = 9004;
                "peer-5" = 9005; "peer-6" = 9006; "peer-7" = 9007 }

# ----------------------------------------------------------------- tiện ích
function Read-DotEnv {
    $vals = @{}
    $path = Join-Path $Root ".env"
    if (-not (Test-Path $path)) { return $vals }
    foreach ($line in Get-Content $path -Encoding UTF8) {
        $line = $line.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) { continue }
        $k, $v = $line.Split("=", 2)
        $v = $v.Trim()
        if ($v -match '^"([^"]*)"' -or $v -match "^'([^']*)'") { $v = $Matches[1] }
        else { $v = ($v -split '\s#', 2)[0].Trim() }
        $vals[$k.Trim()] = $v
    }
    return $vals
}

function Stop-DemoPort([int]$port, [switch]$Quiet) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $conn) {
        if (-not $Quiet) { Write-Host "Không có tiến trình nào đang lắng nghe cổng $port." -ForegroundColor Yellow }
        return
    }
    $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
    if ($Quiet -and $proc -and $proc.ProcessName -notlike "python*") { return }   # chỉ dọn tiến trình python
    Stop-Process -Id $conn.OwningProcess -Force
    if (-not $Quiet) {
        Write-Host "Đã tắt ĐỘT NGỘT tiến trình trên cổng $port (PID $($conn.OwningProcess)), không kịp báo tracker." -ForegroundColor Red
    }
}

function Set-DemoPeer([string]$id, [double]$delay, [double]$corrupt = 0) {
    $env:PEER_ID = $id
    $env:PEER_PORT = "$($DemoPorts[$id])"
    $env:UPLOAD_DELAY = "$delay"
    $env:CORRUPT_RATE = "$corrupt"
}

function Show-DemoHeader([string]$title, [string[]]$lines, [string]$color = "Cyan") {
    Clear-Host
    Write-Host "==== $title ====" -ForegroundColor $color
    foreach ($l in $lines) { Write-Host $l -ForegroundColor DarkGray }
    Write-Host ""
}

# ------------------------------------------------------------ vai trò từng ô
function Start-DemoPane {
    Set-Location $Src
    $script:DemoCmd = $null
    switch ($Role) {
        "tracker" {
            Show-DemoHeader "TRACKER" @("Quản lý metadata và danh sách peer, không bao giờ gửi nội dung file.",
                                        "Lệnh: python -m tracker.server")
            $script:DemoCmd = { python -m tracker.server }
        }
        "peer1" {
            Set-DemoPeer "peer-1" $Delay
            Show-DemoHeader "peer-1 (seeder)" @("Tạo metadata, đăng ký với tracker rồi phục vụ chunk. UPLOAD_DELAY=$Delay s/chunk",
                                                "Lệnh: python -m peer.main seed ..\sample\test.bin") Green
            $script:DemoCmd = { python -m peer.main seed ..\sample\test.bin }
        }
        "peer2" {
            Set-DemoPeer "peer-2" $Delay
            Show-DemoHeader "peer-2 (seeder thứ hai)" @("Cùng file nên cùng FILE_ID=$FileId. UPLOAD_DELAY=$Delay s/chunk",
                                                        "Lệnh: python -m peer.main seed ..\sample\test.bin") Green
            $script:DemoCmd = { python -m peer.main seed ..\sample\test.bin }
        }
        "peer3" {
            Set-DemoPeer "peer-3" $Delay
            Show-DemoHeader "peer-3 (leecher)" @("Tải song song từ nhiều nguồn, đồng thời upload cho peer khác.",
                                                 "Lệnh: python -m peer.main get $FileId") Magenta
            $script:DemoCmd = { python -m peer.main get $FileId }
        }
        "peer4" {
            Set-DemoPeer "peer-4" $Delay
            Show-DemoHeader "peer-4 (leecher)" @("Chạy cùng lúc với peer-3: kịch bản đồng thời.",
                                                 "Lệnh: python -m peer.main get $FileId") Magenta
            $script:DemoCmd = { python -m peer.main get $FileId }
        }
        "evil" {
            Set-DemoPeer "peer-5" 0 1
            Show-DemoHeader "peer-5 (ĐỘC HẠI, chỉ để demo)" @("CORRUPT_RATE=1: luôn gửi chunk bị sửa hỏng.",
                                                              "Lệnh: python -m peer.main seed ..\sample\test.bin") Red
            $script:DemoCmd = { python -m peer.main seed ..\sample\test.bin }
        }
        "tools" {
            Show-DemoHeader "CÔNG CỤ" @(
                "FILE_ID = $FileId",
                "",
                "  peers        xem tracker đang biết những peer nào, mỗi peer có bao nhiêu chunk",
                "  kill1        tắt ĐỘT NGỘT peer-1 (giả lập mất điện) -> SOURCE_FAILED, PEER_EXPIRED",
                "  killport N   tắt đột ngột tiến trình trên cổng N (9001..9007)",
                "  hash         so SHA-256 file gốc với các file đã tải về",
                "  evil         mở thêm ô peer-5 độc hại (gửi chunk sai)",
                "  get6         peer-6 tải file ngay trong ô này -> thấy CHUNK_REJECTED nếu peer-5 đang chạy",
                "  get7         peer-7 tải file; Ctrl+C giữa chừng rồi gõ get7 lần nữa -> RESUME") Yellow
            return
        }
        default { throw "Role không hợp lệ: $Role" }
    }
    Write-Host "Nhấn Enter để chạy (sau này gõ 'go' để chạy lại)..." -ForegroundColor Yellow
    [void](Read-Host)
    go
}

function global:go {
    if ($script:DemoCmd) { & $script:DemoCmd } else { Write-Host "Ô này không có lệnh mặc định." }
}

function global:kill1 { Stop-DemoPort 9001 }
function global:killport([int]$port) { Stop-DemoPort $port }

function global:hash {
    $files = @($Sample) + @(Get-ChildItem (Join-Path $Root "data\*\downloads\test.bin") -ErrorAction SilentlyContinue |
                           ForEach-Object { $_.FullName })
    Get-FileHash -Algorithm SHA256 $files | Format-Table @{ n = "SHA256"; e = { $_.Hash.Substring(0, 24) + "..." } }, Path -AutoSize
}

function global:peers {
    $envs = Read-DotEnv
    $url = if ($envs["TRACKER_URL"]) { $envs["TRACKER_URL"].TrimEnd("/") } else { "http://127.0.0.1:8000" }
    try {
        $list = Invoke-RestMethod -Uri "$url/files/$FileId/peers" -Headers @{ "X-Peer-Token" = $envs["TRACKER_TOKEN"] }
    } catch {
        Write-Host "Không hỏi được tracker: $($_.Exception.Message)" -ForegroundColor Red
        return
    }
    if (-not $list) { Write-Host "Tracker chưa biết peer nào cho file này."; return }
    $list | ForEach-Object {
        [pscustomobject]@{ peer = $_.peer_id; addr = "$($_.host):$($_.port)"; chunks = @($_.chunks).Count; "lần báo cuối (s)" = $_.age }
    } | Format-Table -AutoSize
}

function global:evil {
    $cmd = ". '$Self' -Role evil -FileId $FileId -Delay $Delay"
    if (Get-Command wt.exe -ErrorAction SilentlyContinue) {
        wt.exe -w 0 split-pane -H --title "peer-5 (độc hại)" -d $Src powershell -NoExit -ExecutionPolicy Bypass -Command $cmd
    } else {
        Start-Process powershell -WorkingDirectory $Src -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $cmd)
    }
}

function global:get6 {
    Set-DemoPeer "peer-6" 0
    Push-Location $Src
    try { python -m peer.main get $FileId --exit } finally { Pop-Location }
}

function global:get7 {
    Set-DemoPeer "peer-7" 0
    Push-Location $Src
    try { python -m peer.main get $FileId --exit } finally { Pop-Location }
}

if ($Role) {                      # đang chạy bên trong một ô
    Start-DemoPane
    return
}

# --------------------------------------------------------------- bộ khởi động
Write-Host "== Chuẩn bị demo ==" -ForegroundColor Cyan
if (-not (Get-Command python -ErrorAction SilentlyContinue)) { throw "Không tìm thấy lệnh python." }

if (-not $Keep) {
    Write-Host "Dọn các tiến trình python còn chiếm cổng 8000, 9001-9007 và dữ liệu cũ..."
    foreach ($p in @(8000) + (9001..9007)) { Stop-DemoPort $p -Quiet }
    Start-Sleep -Milliseconds 500
    foreach ($d in @((Join-Path $Root "data"), (Join-Path $Src "data"))) {
        if (Test-Path $d) { Remove-Item -Recurse -Force $d }
    }
}

$envFile = Join-Path $Root ".env"
if (-not (Test-Path $envFile)) {
    $token = -join ((48..57) + (97..122) | Get-Random -Count 32 | ForEach-Object { [char]$_ })
    (Get-Content (Join-Path $Root ".env.example") -Encoding UTF8) -replace '^TRACKER_TOKEN=.*$', "TRACKER_TOKEN=$token" |
        Set-Content $envFile -Encoding UTF8
    Write-Host "Đã tạo .env từ .env.example với TRACKER_TOKEN ngẫu nhiên."
}

$wantBytes = [int64]$SizeMB * 1MB
if (-not (Test-Path $Sample) -or (Get-Item $Sample).Length -ne $wantBytes) {
    Write-Host "Tạo file mẫu $SizeMB MB..."
    Push-Location $Root
    try { python scripts\make_sample.py $SizeMB } finally { Pop-Location }
}

Push-Location $Src
try {
    $fid = (python -c "from common.metadata import create_metadata; print(create_metadata(r'$Sample', 'x')['file_id'])").Trim()
} finally { Pop-Location }
Write-Host "FILE_ID = $fid ($SizeMB MB, UPLOAD_DELAY = $Delay s/chunk)" -ForegroundColor Green

function Get-PaneArgs([string]$r, [string]$title) {
    return @("--title", $title, "-d", $Src, "powershell", "-NoExit", "-ExecutionPolicy", "Bypass",
             "-Command", ". '$Self' -Role $r -FileId $fid -Delay $Delay")
}

if (Get-Command wt.exe -ErrorAction SilentlyContinue) {
    # Bố cục:  | tracker | peer-1 | peer-3 |
    #          | công cụ | peer-2 | peer-4 |
    # Gọi wt.exe từng lệnh một, nghỉ ngắn giữa các lần: gộp tất cả vào một lệnh thì Windows Terminal
    # có lúc chia nhầm ô vì ô mới chưa kịp nhận focus. Mỗi lần chia, focus chuyển sang ô vừa tạo.
    $win = "p2pdemo" + (Get-Date -Format "HHmmss")
    function Invoke-Wt([string[]]$wtArgs) { wt.exe -w $win @wtArgs; Start-Sleep -Milliseconds 900 }
    Invoke-Wt (@("new-tab") + (Get-PaneArgs "tracker" "tracker"))
    Start-Sleep -Milliseconds 600
    Invoke-Wt (@("split-pane", "-H", "-s", "0.5") + (Get-PaneArgs "tools" "công cụ"))
    Invoke-Wt (@("split-pane", "-V", "-s", "0.66") + (Get-PaneArgs "peer2" "peer-2 (seeder)"))
    Invoke-Wt (@("split-pane", "-V", "-s", "0.5") + (Get-PaneArgs "peer4" "peer-4 (leecher)"))
    Invoke-Wt @("move-focus", "up")
    Invoke-Wt (@("split-pane", "-V", "-s", "0.66") + (Get-PaneArgs "peer1" "peer-1 (seeder)"))
    Invoke-Wt (@("split-pane", "-V", "-s", "0.5") + (Get-PaneArgs "peer3" "peer-3 (leecher)"))
    Invoke-Wt @("move-focus", "left")
    Invoke-Wt @("move-focus", "left")
    Write-Host "Đã mở Windows Terminal. Bấm vào từng ô và nhấn Enter theo thứ tự kịch bản." -ForegroundColor Green
} else {
    Write-Host "Không có Windows Terminal (wt.exe): mở 6 cửa sổ PowerShell riêng." -ForegroundColor Yellow
    foreach ($r in "tracker", "peer1", "peer2", "peer3", "peer4", "tools") {
        Start-Process powershell -WorkingDirectory $Src -ArgumentList @(
            "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", ". '$Self' -Role $r -FileId $fid -Delay $Delay")
    }
}
