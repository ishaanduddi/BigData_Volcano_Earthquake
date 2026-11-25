$ErrorActionPreference = "Stop"
$root = "C:\ISHU\Education\Amrita\5th_Sem\BigData\Project\earthquake_bigdata"
$old = Join-Path $root "Team4_Final_Presentation_previous.pptx"
$new = Join-Path $root "Team4_Final_Presentation_FINAL.pptx"
$out = Join-Path $root "Team4_Final_Presentation_MERGED.pptx"

if (-not (Test-Path $old)) { throw "Missing previous deck: $old" }
if (-not (Test-Path $new)) { throw "Missing new deck: $new" }

$ppt = New-Object -ComObject PowerPoint.Application
$ppt.Visible = -1
$ppt.DisplayAlerts = 1

$dest = $ppt.Presentations.Open($old, 0, 0, 0)
$src = $ppt.Presentations.Open($new, -1, 0, 0)

try {
    # Replace the six methodology/benchmark slides (old 21-26) with the six
    # concise slides from the new deck. Insert first, then remove the old set.
    for ($i = 1; $i -le 6; $i++) {
        $src.Slides.Item($i).Copy()
        $dest.Slides.Paste(20 + $i) | Out-Null
    }

    # Old slides 21-26 have shifted to 27-32.
    for ($n = 0; $n -lt 6; $n++) {
        $dest.Slides.Item(27).Delete()
    }

    # Apply the destination deck's design to the six inserted slides so the
    # theme (colours/fonts/background) matches the previous final deck.
    $design = $dest.Slides.Item(20).Design
    for ($i = 21; $i -le 26; $i++) {
        $dest.Slides.Item($i).Design = $design
    }

    $dest.SaveAs($out, 24)
    Write-Output "Merged deck saved: $out"
    Write-Output "Slides: $($dest.Slides.Count)"
}
finally {
    $dest.Close()
    $src.Close()
    $ppt.Quit()
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($dest) | Out-Null
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($src) | Out-Null
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($ppt) | Out-Null
}
