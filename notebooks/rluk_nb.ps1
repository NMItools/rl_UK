Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process -Force
$path = split-path -parent $MyInvocation.MyCommand.Definition
cd "c:\pve\rluk\Scripts"
$script = ".\activate.ps1"
Invoke-Expression "$script"
cd "D:\git\rluk\notebooks"
jupyter notebook
