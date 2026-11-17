<#
.SYNOPSIS
    Chạy MỘT node (tracker hoặc peer) trên máy này, dùng khi triển khai trên nhiều máy trong mạng LAN.

.DESCRIPTION
    Script tự tìm địa chỉ IP LAN của máy, đặt các biến môi trường cần thiết (PEER_HOST, TRACKER_URL, ...)
    rồi chạy tracker hoặc peer. TRACKER_TOKEN đọc từ file .env ở thư mục gốc repo: mọi máy phải dùng
    CÙNG một token. Hướng dẫn đầy đủ: deploy/README.md

.EXAMPLE
    # Mỗi máy, một lần: mở cổng trên tường lửa (PowerShell chạy bằng quyền Administrator)
    powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role firewall

.EXAMPLE
    # Máy A: chạy tracker
    powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role tracker

.EXAMPLE
    # Máy B: chia sẻ file, tracker ở 192.168.1.10
    powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role seed -TrackerIp 192.168.1.10 -PeerId peer-1 -File sample\test.bin

.EXAMPLE
    # Máy C: tải file
    powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role get -TrackerIp 192.168.1.10 -PeerId peer-2 -FileId 61f3063a62931bb1
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("tracker", "seed", "get", "serve", "list", "firewall")]
    [string]$Role,
    [string]$TrackerIp = "",       # IP máy chạy tracker; bỏ trống nếu tracker chạy ngay trên máy này
    [int]$TrackerPort = 8000,
    [string]$PeerId = "",          # mỗi peer một tên riêng: peer-1, peer-2, ...
    [int]$Port = 9001,             # cổng upload của peer
    [string]$File = "",            # dùng với -Role seed
    [string]$FileId = "",          # dùng với -Role get
    [string]$Ip = ""               # ép dùng IP này thay vì tự dò
)

$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

$Root = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$Src = Join-Path $Root "src"

if ($Role -eq "firewall") {          # mở cổng 8000 và 9001-9010 trên tường lửa Windows (cần quyền Administrator)
    $admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator)
    if (-not $admin) { throw "-Role firewall cần chạy PowerShell bằng quyền Administrator." }
    if (-not (Get-NetFirewallRule -DisplayName "P2P DS02" -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName "P2P DS02" -Direction Inbound -Protocol TCP `
            -LocalPort 8000, "9001-9010" -Action Allow -Profile Any | Out-Null
    }
    Write-Host "Đã mở cổng TCP 8000 và 9001-9010 trên tường lửa (quy tắc 'P2P DS02')." -ForegroundColor Green
    return
}

if (-not (Test-Path (Join-Path $Root ".env"))) {
    throw "Chưa có file .env ở $Root. Chép .env.example thành .env và đặt TRACKER_TOKEN giống nhau trên mọi máy."
}

# Dò IP LAN: card mạng đang bật và có default gateway (Wi-Fi hoặc dây), bỏ qua card ảo.
if (-not $Ip) {
    $cfg = Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq "Up" } |
           Select-Object -First 1
    if ($cfg) { $Ip = @($cfg.IPv4Address)[0].IPAddress }
}
if (-not $Ip) { throw "Không dò được IP LAN của máy này. Xem bằng ipconfig rồi truyền vào -Ip." }

if (-not $TrackerIp) { $TrackerIp = $Ip }
$trackerUrl = "http://${TrackerIp}:$TrackerPort"

Write-Host "Máy này: $Ip    Tracker: $trackerUrl" -ForegroundColor Cyan

if ($Role -eq "tracker") {
    $env:TRACKER_HOST = "0.0.0.0"
    $env:TRACKER_PORT = "$TrackerPort"
    Write-Host "Các máy khác dùng: -TrackerIp $Ip" -ForegroundColor Yellow
    Set-Location $Src
    python -m tracker.server
    return
}

if ($Role -eq "list" -and -not $PeerId) { $PeerId = "lister" }
if (-not $PeerId) { throw "Peer cần -PeerId (ví dụ peer-1), mỗi máy một tên khác nhau." }
if (-not (Test-NetConnection -ComputerName $TrackerIp -Port $TrackerPort -InformationLevel Quiet -WarningAction SilentlyContinue)) {
    Write-Host "CẢNH BÁO: chưa kết nối được tới tracker $trackerUrl (tracker chưa chạy, sai IP, hoặc tường lửa chặn)." -ForegroundColor Red
}

$env:PEER_ID = $PeerId
$env:PEER_PORT = "$Port"
$env:PEER_HOST = $Ip              # địa chỉ peer khác dùng để kết nối tới máy này
$env:BIND_HOST = "0.0.0.0"
$env:TRACKER_URL = $trackerUrl

switch ($Role) {
    "seed" {
        if (-not $File) { throw "-Role seed cần -File <đường dẫn file>." }
        $path = (Resolve-Path $File).Path
        Set-Location $Src
        python -m peer.main seed $path
    }
    "get" {
        if (-not $FileId) { throw "-Role get cần -FileId <16 ký tự hex> (giá trị FILE_ID=... mà máy seed in ra)." }
        Set-Location $Src
        python -m peer.main get $FileId
    }
    "serve" { Set-Location $Src; python -m peer.main serve }
    "list" { Set-Location $Src; python -m peer.main list }
}
