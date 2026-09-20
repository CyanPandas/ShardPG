<#
  shardpg_demo.ps1 —— 在 Windows 11 PowerShell 里运行 ShardPG 演示（只用于演示）

  把本文件拷到 Windows 上任意目录，然后：
    .\shardpg_demo.ps1 start        # 准备演示环境（装 demo 函数库、打开 TSO）
    .\shardpg_demo.ps1 sql A        # 窗口 A：连 master 的交互式 psql，提示符 A>
    .\shardpg_demo.ps1 sql B        # 窗口 B：另开一个 PowerShell 窗口运行（演示并发事务）
    .\shardpg_demo.ps1 stop         # 演示结束，恢复演示前的环境

  如果提示"禁止运行脚本"，先执行一次：Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
  也可以不用本文件，直接：ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql A"
#>
param(
    [Parameter(Position = 0)][ValidateSet('start', 'sql', 'stop')][string]$Cmd = 'sql',
    [Parameter(Position = 1)][ValidateSet('A', 'B')][string]$Who = 'A',
    [string]$Server = 'zhanhao@34.31.210.7'
)

# 中文输出不乱码：控制台与管道都用 UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$remote = '~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh'
if ($Cmd -eq 'sql') {
    ssh -t $Server "bash $remote sql $Who"
} else {
    ssh -t $Server "bash $remote $Cmd"
}
