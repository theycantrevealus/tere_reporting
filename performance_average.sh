# python3 -W ignore::DeprecationWarning segmentation_manual.py ~/Downloads/msisdn_wl.csv MSISDN whitelist 6809b2ff819cb94537698818 660e3096279ce3022e5dcb9e 62ffc0fc8a01008799e785bc | awk -F'\\[|\\]' '{print $2}' | xargs
# python3 -W ignore::DeprecationWarning upload_whitelist.py ~/Downloads/msisdn_wl.csv 6809b2ff819cb94537698818 660e3096279ce3022e5dcb9e 62ffc0fc8a01008799e785bc whitelist | awk -F'\\[|\\]' '{print $2}' | xargs



# bash
#!/bin/bash

for i in $(seq 1 5); do
    python3 -W ignore::DeprecationWarning upload_whitelist.py ~/Downloads/msisdn_wl.txt 6809b2ff819cb94537698818 660e3096279ce3022e5dcb9e 62ffc0fc8a01008799e785bc whitelist | awk -F'\\[|\\]' '{print $2}' | xargs
done | awk '
  NR==1 {min=$1; max=$1; sum=$1}
  $1 < min {min=$1}
  $1 > max {max=$1}
  {sum+=$1}
  END {printf "  TUBAGUS\n  Min: %.3f\n  Max: %.3f\n  Avg: %.3f\n", min, max, sum/NR}'
echo ""
echo ""


for i in $(seq 1 5); do
    python3 -W ignore::DeprecationWarning segmentation_manual.py ~/Downloads/msisdn_wl.txt MSISDN whitelist 6809b2ff819cb94537698818 660e3096279ce3022e5dcb9e 62ffc0fc8a01008799e785bc | awk -F'\\[|\\]' '{print $2}' | xargs
done | awk '
  NR==1 {min=$1; max=$1; sum=$1}
  $1 < min {min=$1}
  $1 > max {max=$1}
  {sum+=$1}
  END {printf "  TATANG\n  Min: %.3f\n  Max: %.3f\n  Avg: %.3f\n", min, max, sum/NR}'
echo ""
echo ""

# CPU_MULTIPLIER = 4
# CPU_COUNT = os.cpu_count() or 1
# MAX_WORKERS = min(32, CPU_COUNT * CPU_MULTIPLIER)

