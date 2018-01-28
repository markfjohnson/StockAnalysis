import csv
import argparse


def convert_ratio_group(key):
    key = key.replace(" -> ", "-")
    key = key.replace(" ","")
    return key


def read_morningstar_file(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        row_cnt = 0
        start_block = True
        data = {}
        group_data = {}
        ratio_group = None
        period_list = None

        for rows in reader:

            if len(rows) > 1:
                tt = rows[0]
                if row_cnt > 0:
                    if start_block:
                        ratio_group = cleanup_key(rows[0])
                        period_list = rows[1:len(rows)]
                        start_block = False
                        group_data = {}
                    else:
                        values = {}
                        c = 0
                        for rvalue in rows:
                            if c > 0:
                                period = period_list[c-1]
                                values[period] = convert_numeric(rvalue)
                            c += 1
                        key = cleanup_key(rows[0])
                        group_data[key] = values
            elif len(rows) == 0:
                data[ratio_group] = group_data
                group_data = {}
                start_block = True
            elif len(rows) == 1:
                ratio_group = cleanup_key(rows[0])

            row_cnt = row_cnt + 1

        data[ratio_group] = group_data

        return data


def convert_numeric(v):
    result = 0.0
    if len(v) > 0:
        v = v.replace(",","")
        result = float(v)
    return result


def cleanup_key(s):
    s = s.replace(' & ', '-AND-')
    s = s.replace("%", "PCT")
    s = s.replace("*","")
    s = s.replace(' ', '-')
    s = s.replace('(', '')
    s = s.replace(')', '')
    s = s.replace('\'', '')
    s = s.replace('/', '-')



    return s
##
### Current profitability measures
def fs_fcfta(fcf):
    """ fcfta = free cash flow before taxes and accummulation """
    if (fcf) > 0:
        return 1
    else:
        return 0


def fs_roa(roa):
    """ roa = return on assets"""
    if roa > 0:
        return 1
    else:
        return 0


def f_accrual(net_income_less_extraordinary, cash_flow_operations, total_assets, roa):
    accrual = (net_income_less_extraordinary - cash_flow_operations)/ total_assets
    if (accrual > roa):
        return 1
    else:
        return 0


##
### Stability Measures
def fs_delta_lever(lt_debt_prior, lt_debt_cur, total_assets_prior,total_assets_cur):
    """
    lt_debt_last = long term debt prior period
    lt_debt_cur = long term debt this period
    total_assets_last = total assets prior period
    total_assets_cur = total assdets current period
    """
    delta_lever = (lt_debt_cur/total_assets_cur) - (lt_debt_prior/total_assets_prior)
    if delta_lever > 0:
        return 1
    else:
        return 0


def fs_delta_liquid(current_ratio_prior, current_ratio_cur):
    delta_liquid = (current_ratio_cur)-(current_ratio_prior)
    if delta_liquid > 0:
        return 1
    else:
        return 0


def fs_delta_neqiss(equity_repurchases, equity_issuance):
    if equity_repurchases > equity_issuance:
        return 1
    else:
        return 0


##
### Recent Operational Improvements
def fs_delta_roa(roa_prior, roa_current):
    delta_roa = roa_current - roa_prior
    if delta_roa > 0:
        return 1
    else:
        return 0


def fs_delta_fcfta(fcfta_prior, fcfta_current):
    delta_fcfta = fcfta_current - fcfta_prior
    if delta_fcfta > 0:
        return 1
    else:
        return 0


def fs_delta_margin(gm_cur, gm_prior):
    delta_margin = (gm_cur - gm_prior)
    if delta_margin > 0:
        return 1
    else:
        return 0


def fs_delta_turn(asset_turnover_cur, asset_turnover_prior):
    delta_turn = (asset_turnover_cur - asset_turnover_prior)
    if delta_turn > 0:
        return 1
    else:
        return 0

def read_morningstar_data():
    parser = argparse.ArgumentParser(description='Calculate FS_SCORE for a given stock')
    parser.add_argument('--ratios', help='Income Statement', required=True)

    args = parser.parse_args()

    print(args)

    ratios_data = read_morningstar_file(args.ratios)
    return ratios_data

def main():
    ratio_data = read_morningstar_data()
    current_period = "TTM"
    prior_period = "2016-12"
    current_qtr = "Latest Qtr"

    ## Note the below variables may differ for each equity
    fcf_cur = ratio_data["Cash-Flow-Ratios"]["Free-Cash-Flow-Growth-PCT-YOY"][current_period]
    fcf_prior = ratio_data["Cash-Flow-Ratios"]["Free-Cash-Flow-Growth-PCT-YOY"][prior_period]
    roa_cur = ratio_data['Profitability']["Return-on-Assets-PCT"][current_period]
    roa_prior = ratio_data['Profitability']["Return-on-Assets-PCT"][prior_period]
    net_income_less_extraordinary = ratio_data['Financials']['Net-Income-DKK-Mil'][current_period]
    cash_flow_operations = ratio_data['Financials']['Free-Cash-Flow-DKK-Mil'][current_period]
    # TODO Fix..no ratio should be here
    total_assets_cur = ratio_data['Balance-Sheet-Items-in-PCT']['Total-Assets'][current_qtr]
    total_assets_prior = ratio_data['Balance-Sheet-Items-in-PCT']['Total-Assets'][current_qtr]
    lt_debt_prior = ratio_data['Balance-Sheet-Items-in-PCT']['Long-Term-Debt'][current_qtr]
    lt_debt_cur = ratio_data['Balance-Sheet-Items-in-PCT']['Long-Term-Debt'][current_qtr]
    current_ratio_prior = ratio_data['Liquidity-Financial-Health']['Current-Ratio'][prior_period]
    current_ratio_cur = ratio_data['Liquidity-Financial-Health']['Current-Ratio'][current_qtr]
    # TODO Fix the next 2 lines
    equity_repurchases_cur = None
    equity_issuance_cur = None
    gross_margin_PCT_cur = ratio_data['Financials']['Gross-Margin-PCT'][current_period]
    gross_margin_PCT_prior = ratio_data['Financials']['Gross-Margin-PCT'][prior_period]
    asset_turnover_cur = ratio_data['Efficiency']['Asset-Turnover'][current_period]
    asset_turnover_prior = ratio_data['Efficiency']['Asset-Turnover'][prior_period]


    print fcf_cur
    # TODO fix fs_delta_fcfta, fs_delta_neqiss
    financial_strength = fs_fcfta(fcf_prior) + fs_roa(roa_cur)+\
               f_accrual(net_income_less_extraordinary, cash_flow_operations, total_assets_cur, roa_cur)

    print "Financial Strength"
    print "FCFTA","ROA","ACCURAL","Score"
    print fs_fcfta(fcf_prior), fs_roa(roa_cur), f_accrual(net_income_less_extraordinary, \
                                                          cash_flow_operations, \
                                                          total_assets_cur, roa_cur), \
        financial_strength

    print ""
    print "Stability:"
    stability_score = fs_delta_lever(lt_debt_prior, lt_debt_cur, total_assets_prior, total_assets_cur) \
                      + fs_delta_liquid(current_ratio_prior, current_ratio_cur) \
                      + fs_delta_neqiss(equity_repurchases_cur, equity_issuance_cur)
    print "Delta_Leverage","Delta_Liquidity","NEQISS","Score"
    print fs_delta_lever(lt_debt_prior, lt_debt_cur, total_assets_prior,total_assets_cur), \
        fs_delta_liquid(current_ratio_prior, current_ratio_cur), \
        fs_delta_neqiss(equity_repurchases_cur, equity_issuance_cur), \
        stability_score

    print ""
    print "Recent Operational Improvements:"
    op_improvements = fs_delta_roa(roa_prior, roa_cur) \
               + fs_delta_fcfta(fcf_prior, fcf_cur) \
               + fs_delta_margin(gross_margin_PCT_cur, gross_margin_PCT_prior) \
               + fs_delta_turn(asset_turnover_cur, asset_turnover_prior)
    print "Delta_ROA","Delta_FCFTA","Delta_Margin","Delta_Asset_Turns","Score"
    print fs_delta_roa(roa_prior, roa_cur), fs_delta_fcfta(fcf_prior, fcf_cur), \
        fs_delta_margin(gross_margin_PCT_cur, gross_margin_PCT_prior), fs_delta_turn(asset_turnover_cur, asset_turnover_prior)

    fs_score = financial_strength + stability_score + op_improvements
    #
    print "The fs_score is: {}".format(fs_score)

if __name__ == "__main__": main()


