"""REVOKE POINT V1"""
import subprocess

report_entry = []
LOG = "testing_cache.log"
target_source = f"revoke/{LOG}"
target_log = f"logs/{LOG.replace('.csv', '.log')}"

# tail -n +2 namafile | awk -F '|' '$3 >= $4 {print}' | wc -l
# success_row = subprocess.run(["tail", "-n", "+2", target_log, "|", "awk", "-F", "'|'", "'$3>=$4{print}'", "|", "wc", "-l"], shell=True, capture_output=True, text=True, check=True)
success_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
success_row_p2 = subprocess.Popen(["awk", "-F", "|", "$3>=$4{print}"], stdin=success_row_p1.stdout, stdout=subprocess.PIPE)
success_row_p1.stdout.close()
success_row_p3 = subprocess.Popen(["wc", "-l"], stdin=success_row_p2.stdout, stdout=subprocess.PIPE)
success_row_p2.stdout.close()
success_row = success_row_p3.communicate()[0].decode('utf-8') or 0

# tail -n +2 namafile | awk -F '|' '$3 >= $4 {print}' | awk -F '{total += $5}END{print total}'
# success_total_redeem = subprocess.run(["tail", "-n", "+2", target_log, "|", "awk", "-F", "'|'", "'$3>=$4{print}'", "|", "awk", "-F", "'{total+=$5}END{print total}'"], capture_output=True, text=True, check=True)
success_total_redeem_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
success_total_redeem_p2 = subprocess.Popen(["awk", "-F", "|", "$3>=$4{print}"], stdin=success_total_redeem_p1.stdout, stdout=subprocess.PIPE)
success_total_redeem_p1.stdout.close()
success_total_redeem_p3 = subprocess.Popen(["awk", "-F", "{total+=$5}END{print total}"], stdin=success_total_redeem_p2.stdout, stdout=subprocess.PIPE)
success_total_redeem_p2.stdout.close()
success_total_redeem = success_total_redeem_p3.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | awk -F '|' '$3 >= $4 {print}' | awk -F '{total += $4}END{print total}'
# success_revoke = subprocess.run(["tail", "-n", "+2", target_log, "|", "awk", "-F", "'|'", "'$3>=$4{print}'", "|", "awk", "-F", "'{total+=$4}END{print total}'"], capture_output=True, text=True, check=True)
success_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
success_revoke_p2 = subprocess.Popen(["awk", "-F", "|", "$3>=$4{print}"], stdin=success_revoke_p1.stdout, stdout=subprocess.PIPE)
success_revoke_p1.stdout.close()
success_revoke_p3 = subprocess.Popen(["awk", "-F", "{total+=$4}END{print total}"], stdin=success_revoke_p2.stdout, stdout=subprocess.PIPE)
success_revoke_p2.stdout.close()
success_revoke = success_revoke_p3.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | grep Skip | wc -l
# fail_row = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "wc", "-l"], capture_output=True, text=True, check=True)
fail_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
fail_row_p2 = subprocess.Popen(["grep", "Skip"], stdin=fail_row_p1.stdout, stdout=subprocess.PIPE)
fail_row_p1.stdout.close()
fail_row_p3 = subprocess.Popen(["wc", "-l"], stdin=fail_row_p2.stdout, stdout=subprocess.PIPE)
fail_row = fail_row_p3.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | grep Skip | awk -F '{total += $5}END{print total}'
# fail_total_redeem = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'{total+=$5}END{print total}'"], capture_output=True, text=True, check=True)
fail_total_redeem_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
fail_total_redeem_p2 = subprocess.Popen(["grep", "Skip"], stdin=fail_total_redeem_p1.stdout, stdout=subprocess.PIPE)
fail_total_redeem_p1.stdout.close()
fail_total_redeem_p3 = subprocess.Popen(["awk", "-F", "{total+=$5}END{print total}"], stdin=fail_total_redeem_p2.stdout, stdout=subprocess.PIPE)
fail_total_redeem = fail_total_redeem_p3.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | grep Skip | awk -F '{total += $4}END{print total}'
# fail_revoke = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'{total+=$4}END{print total}'"], capture_output=True, text=True, check=True)
fail_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
fail_revoke_p2 = subprocess.Popen(["grep", "Skip"], stdin=fail_revoke_p1.stdout, stdout=subprocess.PIPE)
fail_revoke_p1.stdout.close()
fail_revoke_p3 = subprocess.Popen(["awk", "-F", "{total+=$4}END{print total}"], stdin=fail_revoke_p2.stdout, stdout=subprocess.PIPE)
fail_revoke_p2.stdout.close()
fail_revoke = fail_revoke_p3.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | grep -v Skip | awk -F '|' '$3 < $4 {print}' | wc -l
# partial_row = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'|'", "'$3<$4 {print}'", "|", "wc", "-l"], capture_output=True, text=True, check=True)
partial_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
partial_row_p2 = subprocess.Popen(["grep", "Skip"], stdin=partial_row_p1.stdout, stdout=subprocess.PIPE)
partial_row_p1.stdout.close()
partial_row_p3 = subprocess.Popen(["awk", "-F", "|", "$3<$4 {print}"], stdin=partial_row_p2.stdout, stdout=subprocess.PIPE)
partial_row_p2.stdout.close()
partial_row_p4 = subprocess.Popen(["wc", "-l"], stdin=partial_row_p3.stdout, stdout=subprocess.PIPE)
partial_row_p3.stdout.close()
partial_row = partial_row_p4.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | grep -v Skip | awk -F '|' '$3 < $4 {print}' | awk -F '{total += $5}END{print total}'
# partial_total_redeem = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'|'", "'$3<$4 {print}'", "|", "awk", "-F", "'{total+=$5}END{print total}'"], capture_output=True, text=True, check=True)
partial_total_redeem_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
partial_total_redeem_p2 = subprocess.Popen(["grep", "Skip"], stdin=partial_total_redeem_p1.stdout, stdout=subprocess.PIPE)
partial_total_redeem_p1.stdout.close()
partial_total_redeem_p3 = subprocess.Popen(["awk", "-F", "|", "$3<$4 {print}"], stdin=partial_total_redeem_p2.stdout, stdout=subprocess.PIPE)
partial_total_redeem_p2.stdout.close()
partial_total_redeem_p4 = subprocess.Popen(["awk", "-F", "{total+=$5}END{print total}"], stdin=partial_total_redeem_p3.stdout, stdout=subprocess.PIPE)
partial_total_redeem_p3.stdout.close()
partial_total_redeem = partial_total_redeem_p4.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | grep -v Skip | awk -F '|' '$3 < $4 {print}' | awk -F '{total += $4}END{print total}'
# partial_revoke = subprocess.run(["tail", "-n", "+2", target_log, "|", "grep", "Skip", "|", "awk", "-F", "'|'", "'$3<$4 {print}'", "|", "awk", "-F", "'{total+=$4}END{print total}'"], capture_output=True, text=True, check=True)
partial_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_log], stdout=subprocess.PIPE)
partial_revoke_p2 = subprocess.Popen(["grep", "Skip"], stdin=partial_revoke_p1.stdout, stdout=subprocess.PIPE)
partial_revoke_p1.stdout.close()
partial_revoke_p3 = subprocess.Popen(["awk", "-F", "|", "$3<$4 {print}"], stdin=partial_revoke_p2.stdout, stdout=subprocess.PIPE)
partial_revoke_p2.stdout.close()
partial_revoke_p4 = subprocess.Popen(["awk", "-F", "{total+=$4}END{print total}"], stdin=partial_revoke_p3.stdout, stdout=subprocess.PIPE)
partial_revoke_p3.stdout.close()
partial_revoke = partial_revoke_p4.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | wc -l
# source_row = subprocess.run(["tail", "-n", "+2", target_source, "|", "wc", "-l"], capture_output=True, text=True, check=True)
source_row_p1 = subprocess.Popen(["tail", "-n", "+2", target_source], stdout=subprocess.PIPE)
source_row_p2 = subprocess.Popen(["wc", "-l"], stdin=source_row_p1.stdout, stdout=subprocess.PIPE)
source_row_p1.stdout.close()
source_row = source_row_p2.communicate()[0].decode('utf-8') or 0
# =========================================================================

# tail -n +2 namafile | awk -F '|' '{total += $8}END{print total}'
# source_revoke = subprocess.run(["tail", "-n", "+2", target_source, "|", "awk", "-F", "'|'", "'{total+=$8}END{print total}'"], capture_output=True, text=True, check=True)
source_revoke_p1 = subprocess.Popen(["tail", "-n", "+2", target_source], stdout=subprocess.PIPE)
source_revoke_p2 = subprocess.Popen(["awk", "-F", "|", "{total+=$8}END{print total}"], stdin=source_revoke_p1.stdout, stdout=subprocess.PIPE)
source_revoke_p1.stdout.close()
source_revoke = source_revoke_p2.communicate()[0].decode('utf-8') or 0
# =========================================================================

db_row = 0
db_total_redeem = 0

report_entry.append([
    target_source,
    success_row,
    success_total_redeem,
    success_revoke,
    fail_row,
    fail_total_redeem,
    fail_revoke,
    partial_row,
    partial_total_redeem,
    partial_revoke,
    source_row,
    source_revoke,
    db_row,
    db_total_redeem
])

print(report_entry)