# create an alias called "ve" that looks for the relative location of -Value
# and runs it.
New-Alias -Name rluk -Value c:/pve/rluk/Scripts/activate.ps1

# Export the alias to a script.
Export-Alias -Name rluk -Path "rluk.ps1" -As Script

# Make sure this scripts runs each time you start a powershell session.
Add-Content -Path $Profile -Value (Get-Content rluk.ps1)