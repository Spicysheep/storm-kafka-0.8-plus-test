import storm
from decimal import Decimal, getcontext
getcontext().prec = 12 # Sets the decimal place
from math import log, exp

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

def calculate_score(line):
    line = line.strip() # Remove whitespace
    line = line.split(",")
    ent_cust_id = line[0]
    logit = intercept + \
        Decimal(line[1]) * coeffs["Bankruptcy_Index"] + \
        Decimal(line[2]) * coeffs["RE_Scurd_Indicator"] + \
        Decimal(line[3]) * coeffs["FICO_Score"] + \
        Decimal(line[4]) * coeffs["LOC_Utlz_Scale"] + \
        Decimal(line[5]) * coeffs["Deposit_Bal_Tier"] + \
        Decimal(line[6]) * coeffs["Deposit_Bal_Trend"] + \
        Decimal(line[7]) * coeffs["Curr_PD_Days_Cnt_S"] + \
        Decimal(line[8]) * coeffs["Pay_Idx_100_Indicator"]
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
        line = tup.values[0]
        score = calculate_score(line)
        storm.emit([score])
        
SplitSentenceBolt().run()
