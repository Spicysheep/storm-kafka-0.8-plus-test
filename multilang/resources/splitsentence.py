import storm
from decimal import Decimal, getcontext
getcontext().prec = 12 # Sets the decimal place
from math import log, exp

import json
from subprocess import Popen, PIPE

coeffs = {}
coeffs["Bankruptcy_Index"] = Decimal("-0.00422064")
coeffs["RE_Scurd_Indicator"] = Decimal("-0.64315405")
coeffs["FICO_Score"] = Decimal("-0.00548053")
coeffs["LOC_Utlz_Scale"] = Decimal("0.55422165")
coeffs["Deposit_Bal_Tier"] = Decimal("-0.18463059")
coeffs["Deposit_Bal_Trend"] = Decimal("-0.33357832")
coeffs["Curr_PD_Days_Cnt_S"] = Decimal("1.20648812")
coeffs["Pay_Idx_100_Indicator"] = Decimal("0")
intercept = Decimal("1.50227124")

def calculate_score(model_input):
    ent_cust_id = model_input["Account_Id"]
    logit = intercept + \
        Decimal(str(model_input["Bankruptcy_Index"])) * coeffs["Bankruptcy_Index"] + \
        Decimal(str(model_input["RE_Scurd_Indicator"])) * coeffs["RE_Scurd_Indicator"] + \
        Decimal(str(model_input["FICO_Score"])) * coeffs["FICO_Score"] + \
        Decimal(str(model_input["LOC_Utlz_Scale"])) * coeffs["LOC_Utlz_Scale"] + \
        Decimal(str(model_input["Deposit_Bal_Tier"])) * coeffs["Deposit_Bal_Tier"] + \
        Decimal(str(model_input["Deposit_Bal_Trend"])) * coeffs["Deposit_Bal_Trend"] + \
        Decimal(str(model_input["Curr_PD_Days_Cnt_S"])) * coeffs["Curr_PD_Days_Cnt_S"] + \
        Decimal(str(model_input["Pay_Idx_100_Indicator"])) * coeffs["Pay_Idx_100_Indicator"]
    # Downsample adjustment only for ROR
    adjustment = Decimal(str(log(0.51578631)))
    logit = logit + adjustment
    # Logistic regression formula
    score = 1/(1+exp(-1*(logit)))
    # Limit the number of digits
    score = str(score)[:12]
    score_output_record = (ent_cust_id,score)
    return score

class SplitSentenceBolt(storm.BasicBolt):

    def process(self, tup):
        acct_id = tup.values[0]
        p = Popen(['curl http://10.94.98.77:19237/v1/ror?acct_id='+acct_id],stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
        output, err = p.communicate()
        rc = p.returncode
        model_input = json.loads(output)
        score = calculate_score(model_input)
        storm.emit([score])

SplitSentenceBolt().run()
