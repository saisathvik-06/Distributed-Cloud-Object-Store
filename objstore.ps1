# Shortcut for the command-line client:   .\objstore.ps1 <command> [arguments]
# Files are read from and written to the demo\work folder (mounted at /work in the client container).
#
#   .\objstore.ps1 gen 5.5MB big.bin
#   .\objstore.ps1 upload big.bin --meta owner=sai
#   .\objstore.ps1 list
#   .\objstore.ps1 cluster

Push-Location $PSScriptRoot
try {
    docker compose run --rm client @args
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
