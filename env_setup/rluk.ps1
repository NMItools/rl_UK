# mkdir c:\pve\rluk
# python -m venv c:\pve\rluk
$path = split-path -parent $MyInvocation.MyCommand.Definition
cd "c:\pve\rluk\Scripts"
$script = ".\activate.ps1"
Invoke-Expression "$script"
cd "D:\git\rluk\python"
# cd "D:\git\rluk\env_setup"
# python -m pip install -r requirements.txt
