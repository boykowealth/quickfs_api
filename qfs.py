class QFS():

    def __init__(self, api_key, timeout: int = 600):
        self.api_key = api_key
        self.timeout = timeout
        self.HOST = 'https://public-api.quickfs.net/v1'
        self.resp = None
        
        self.headers = {
                'X-QFS-API-Key': api_key
            }
        
        self.endpoint_pivot = None
        self.QUICKFS_KEYS = ['data']
        self.keys_bool = False
        self.response_key = None
        self.query_params_names = ["period"]
        self.name_error = False
        self.request_body = None
        self.session = requests.Session()  # Use a session for connection pooling
        
    
    def __handle_response(self, query_params: Dict[str, str]={}):
        try:
            if self.request_body is None:
                self.resp = self.session.get(self.endpoint_pivot, params=query_params, headers=self.headers, timeout=self.timeout)
            else:
                self.resp = self.session.post(self.endpoint_pivot, json=self.request_body, headers=self.headers, timeout=self.timeout)
                self.request_body = None  # Reset the request_body after the POST request

            print(" --- Status:", self.resp.status_code, end="\r")
            self.__response_key_finder(self.resp)
            
            if self.resp.status_code == 200:
                return self.resp.json().get(self.response_key, {})
            else:
                self.resp.raise_for_status()  # Raise an error for bad status codes

        finally:
            if self.resp:
                self.resp.close()  # Close response object to free resources

    def __endpoint_builder(self, endpoint: str):
        self.endpoint_pivot = f"{self.HOST}{endpoint}"
        
    
    def __param_checker(self, items_):
        for key, value in items_:
            if key not in self.query_params_names:
                logging.error(f"The parameter {key} is not valid.")
                self.name_error = True
                
    def __response_key_finder(self, response):
        try:
            for key in response.json().keys():
                if key in self.QUICKFS_KEYS:
                    self.response_key = key
                    return
            logging.error("None of the expected keys found in the response.")
            self.response_key = None
        except Exception as e:
            logging.error(f"Error parsing response JSON: {e}")
            self.response_key = None

    # ------------------------------
    # Datapoints
    # ------------------------------
    
    def data(self, symbol: str, metric: str, **query_params):

        self.__endpoint_builder(f"/data/{symbol.upper()}/{metric.lower()}")
        
        self.__param_checker(items_=query_params.items())
        
        if self.name_error:
            self.name_error = False
            return
        
        return self.__handle_response(query_params)

class BW():
    ## Data Pull
    def get_list():
        data = pd.read_feather(ENTER LIST HERE)
        data = pd.DataFrame(data)
        return data

    def get_qfs_data(qfs_id, metric, interval, UOM):
        print(qfs_id,"--",metric, end='/r')
        rows = 20
        default_value = 0
        data_arr = qfs.data(qfs_id, metric, period=interval)
        
        if data_arr is None:
            return np.full(rows, default_value)
        else:
            return np.array(data_arr) / UOM

    def data(interval):
        print("Starting Data Connection...")
        start_time = time.time()
        data = BW.get_list()

        ## START -> FS
        UOM = 1000000
        rows = 20

        time_data = time.time()
        print("Company List Retrieved ---", round(time_data-start_time, 2),"secs")

        ## START -> DATE
        print("Starting Date Pull...")
        data['DATE'] = data['QFS_ID'].apply(lambda qfs_id: pd.to_datetime(qfs.data(qfs_id, 'original_filing_date', period=interval) or np.zeros(rows), format='%Y-%m-%d', errors='coerce')).fillna(pd.Timestamp('1900-01-01'), inplace=True)
        data = data.explode(['DATE']).reset_index(drop=True)
        time_date = time.time()
        print("Dates Recorded ---", round((time_date-time_data)/3600, 2),"hrs")

        ## START -> INCOME STATEMENT
        print("Starting Income Statement...")
        revenue = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'revenue', interval, UOM))
        cogs = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cogs', interval, UOM))
        gross_profit = revenue - cogs
        
        sga = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'sga', interval, UOM))
        rnd = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'rnd', interval, UOM))
        special_charges = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'special_charges', interval, UOM))
        other_opex = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_opex', interval, UOM))
        total_opex = sga + rnd + special_charges + other_opex
        operating_income = gross_profit - total_opex

        interest_income = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'interest_income', interval, UOM))
        interest_expense = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'interest_expense', interval, UOM))
        net_interest_income = interest_income + interest_expense
        pretax_income = operating_income + net_interest_income

        income_tax = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'income_tax', interval, UOM))
        net_income_continuing = pretax_income - income_tax
        
        net_income_discontinued = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'net_income_discontinued', interval, UOM))
        income_allocated_to_minority_interest = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'income_allocated_to_minority_interest', interval, UOM))
        other_income_statement_items = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_income_statement_items', interval, UOM))
        net_income = net_income_continuing + net_income_discontinued - income_allocated_to_minority_interest + other_income_statement_items
        
        preferred_dividends = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'preferred_dividends', interval, UOM))
        net_income_available_to_shareholders = net_income - preferred_dividends
        
        shares_basic = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'shares_basic', interval, UOM))
        shares_diluted = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'shares_diluted', interval, UOM))
        eps_basic = net_income / shares_basic
        eps_diluted = net_income / shares_diluted
        da_income_statement_suplemental = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'da_income_statement_supplemental', interval, UOM))

        time_income = time.time()
        print("Income Statement Complete ---", round((time_income-time_date)/3600, 2),"hrs")
        ## END -> INCOME STATEMENT

        ## START -> BALANCE SHEET
        print("Starting Balance Sheet...")
        cash_and_equivalents = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cash_and_equiv', interval, UOM))
        st_investments = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'st_investments', interval, UOM))
        receivables = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'receivables', interval, UOM))
        inventories = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'inventories', interval, UOM))
        other_current_assets = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_current_assets', interval, UOM))
        total_current_assets = cash_and_equivalents + st_investments + receivables + inventories + other_current_assets

        equity_and_other_investments = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'equity_and_other_investments', interval, UOM))
        ppe_gross = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'ppe_gross', interval, UOM))
        accumlated_depreciation = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'accumulated_depreciation', interval, UOM))
        ppe_net = ppe_gross - accumlated_depreciation

        intangible_assets = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'intangible_assets', interval, UOM))
        goodwill = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'goodwill', interval, UOM))
        other_lt_assets = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_lt_assets', interval, UOM))
        total_non_current_assets = equity_and_other_investments + ppe_net + intangible_assets + goodwill + other_lt_assets
        total_assets = total_current_assets + total_non_current_assets

        accounts_payable = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'accounts_payable', interval, UOM))
        tax_payable = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'tax_payable', interval, UOM))
        current_accrued_liabilities = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'current_accrued_liabilities', interval, UOM))
        st_debt = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'st_debt', interval, UOM))
        current_deferred_revenue = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'current_deferred_revenue', interval, UOM))
        current_deferred_tax_liability = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'current_deferred_tax_liability', interval, UOM))
        current_capital_leases = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'current_capital_leases', interval, UOM))
        other_current_liabilities = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_current_liabilities', interval, UOM))
        total_current_liabilities = accounts_payable + tax_payable + current_accrued_liabilities + st_debt + current_deferred_revenue + current_deferred_tax_liability + current_capital_leases + other_current_liabilities

        lt_debt = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'lt_debt', interval, UOM))
        noncurrent_capital_leases = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'noncurrent_capital_leases', interval, UOM))
        pension_liabilities = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'pension_liabilities', interval, UOM))
        noncurrent_deferred_revenue = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'noncurrent_deferred_revenue', interval, UOM))
        other_lt_liabilities = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_lt_liabilities', interval, UOM))
        total_non_current_liabilities = lt_debt + noncurrent_capital_leases + pension_liabilities + noncurrent_deferred_revenue + other_lt_liabilities
        total_liabilities = total_current_liabilities + total_non_current_liabilities

        common_stock = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'common_stock', interval, UOM))
        preferred_stock = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'preferred_stock', interval, UOM))
        retained_earnings = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'retained_earnings', interval, UOM))
        aoci = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'aoci', interval, UOM))
        apic = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'apic', interval, UOM))
        treasury_stock = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'treasury_stock', interval, UOM))
        other_equity = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'other_equity', interval, UOM))
        minority_interest_liability = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'minority_interest_liability', interval, UOM))
        total_equity = common_stock + preferred_stock + retained_earnings + aoci + apic + treasury_stock + other_equity - minority_interest_liability
        total_liabilities_and_equity = total_liabilities + total_equity

        time_balance = time.time()
        print("Balance Sheet ---",round((time_balance-time_income)/3600, 2),"hrs")
        ## END -> BALANCE SHEET
        
        ## START -> CASH FLOW
        print("Starting Cash Flow Statement...")
        cfo_net_income = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_net_income', interval, UOM))
        cfo_da = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_da', interval, UOM))
        cfo_recievables = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_receivables', interval, UOM))
        cfo_inventory = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_inventory', interval, UOM))
        cfo_prepaid_expenses = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_prepaid_expenses', interval, UOM))
        cfo_other_working_capital = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_other_working_capital', interval, UOM))
        cfo_change_in_working_capital = cfo_recievables + cfo_inventory + cfo_prepaid_expenses + cfo_other_working_capital
        
        cfo_deferred_tax = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_deferred_tax', interval, UOM))
        cfo_stock_comp = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_stock_comp', interval, UOM))
        cfo_other_noncash_items = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfo_other_noncash_items', interval, UOM))
        cf_cfo = cfo_net_income + cfo_da + cfo_deferred_tax + cfo_stock_comp + cfo_other_noncash_items - cfo_change_in_working_capital

        cfi_ppe_purchases = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_ppe_purchases', interval, UOM))
        cfi_ppe_sales = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_ppe_sales', interval, UOM))
        cfi_ppe_net = cfi_ppe_sales - cfi_ppe_purchases

        cfi_acquistions = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_acquisitions', interval, UOM))
        cfi_divestitures = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_divestitures', interval, UOM))
        cfi_acquistions_net = cfi_acquistions - cfi_divestitures

        cfi_investment_purchases = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_investment_purchases', interval, UOM))
        cfi_investment_sales = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_investment_sales', interval, UOM))
        cfi_investment_net = cfi_investment_sales - cfi_investment_purchases

        cfi_intangibles_net = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_intangibles_net', interval, UOM))
        cfi_other = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cfi_other', interval, UOM))
        cf_cfi = cfi_ppe_net + cfi_acquistions_net + cfi_investment_net + cfi_intangibles_net + cfi_other

        cff_common_stock_issued = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_common_stock_issued', interval, UOM))
        cff_common_stock_repurchased = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_common_stock_repurposed', interval, UOM))
        cff_common_stock_net = cff_common_stock_issued - cff_common_stock_repurchased

        cff_pfd_issued = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_pfd_issued', interval, UOM))
        cff_pfd_repurchased = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_pfd_repurposed', interval, UOM))
        cff_pfd_net = cff_pfd_issued - cff_pfd_repurchased

        cff_debt_issued = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_debt_issued', interval, UOM))
        cff_debt_repaid = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_debt_repaid', interval, UOM))
        cff_debt_net = cff_debt_issued - cff_debt_repaid

        cff_dividend_paid = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_dividend_paid', interval, UOM))
        cff_other = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cff_other', interval, UOM))
        cf_cff = cff_common_stock_net + cff_pfd_net + cff_debt_net - cff_dividend_paid + cff_other

        cf_forex = data['QFS_ID'].apply(lambda qfs_id: BW.get_qfs_data(qfs_id, 'cf_forex', interval, UOM))
        cf_net_changes_in_cash = cf_cfo + cf_cfi + cf_cff + cf_forex
        time_cashflow = time.time()
        print("Cash Flow ---",round((time_cashflow-time_balance)/3600, 2),"hrs")
        ## END -> Cash Flow 

        ## START -> Caclulated Rows

        ## END -> Calculated Rows

        ## -----EXPLODE DATE INTO ROWS START---- ##
        print("Starting To Join Data...")
        time_data_join = time.time()
        
        data_point = pd.DataFrame({

            'REVENUE': revenue,
            'COGS': cogs,
            'GROSS_PROFIT': gross_profit,
            'SGA': sga,
            'RND': rnd,
            'SPECIAL_CHARGES': special_charges,
            'OTHER_OPEX': other_opex,
            'TOTAL_OPEX': total_opex,
            'OPERATING_INCOME': operating_income,
            'INTEREST_INCOME': interest_income,
            'INTEREST_EXPENSE': interest_expense,
            'NET_INTEREST_INCOME': net_interest_income,
            'INCOME_TAX': income_tax,
            'NET_INCOME_CONTINUING': net_income_continuing,
            'NET_INCOME_DISCONTINUED': net_income_discontinued,
            'INCOME_ALLOCATED_TO_MINORITY_INTEREST': income_allocated_to_minority_interest,
            'OTHER_INCOME_STATEMENT_ITEMS': other_income_statement_items,
            'NET_INCOME': net_income,
            'PREFERRED_DIVIDENDS': preferred_dividends,
            'NET_INCOME_AVAILABLE_TO_SHAREHOLDERS': net_income_available_to_shareholders,
            'SHARES_BASIC': shares_basic,
            'SHARES_DILUTED': shares_diluted,
            'EPS_BASIC': eps_basic,
            'EPS_DILUTED': eps_diluted,
            'DA_INCOME_STATEMENT_SUPPLEMENTAL': da_income_statement_suplemental,
            'CASH_AND_EQUIV': cash_and_equivalents,
            'ST_INVESTMENTS': st_investments,
            'RECEIVABLES': receivables,
            'INVENTORIES': inventories,
            'OTHER_CURRENT_ASSETS': other_current_assets,
            'TOTAL_CURRENT_ASSETS': total_current_assets,
            'EQUITY_AND_OTHER_INVESTMENTS': equity_and_other_investments,
            'PPE_GROSS': ppe_gross,
            'ACCUMULATED_DEPRECIATION': accumlated_depreciation,
            'PPE_NET': ppe_net,
            'INTANGIBLE_ASSETS': intangible_assets,
            'GOODWILL': goodwill,
            'OTHER_LT_ASSETS': other_lt_assets,
            'TOTAL_NON_CURRENT_ASSETS': total_non_current_assets,
            'TOTAL_ASSETS': total_assets,
            'ACCOUNTS_PAYABLE': accounts_payable,
            'TAX_PAYABLE': tax_payable,
            'CURRENT_ACCRUED_LIABILITIES': current_accrued_liabilities,
            'ST_DEBT': st_debt,
            'CURRENT_DEFERRED_REVENUE': current_deferred_revenue,
            'CURRENT_DEFERRED_TAX_LIABILITY': current_deferred_tax_liability,
            'CURRENT_CAPITAL_LEASES': current_capital_leases,
            'OTHER_CURRENT_LIABILITIES': other_current_liabilities,
            'TOTAL_CURRENT_LIABILITIES': total_current_liabilities,
            'LT_DEBT': lt_debt,
            'NONCURRENT_CAPITAL_LEASES': noncurrent_capital_leases,
            'PENSION_LIABILITIES': pension_liabilities,
            'NONCURRENT_DEFERRED_REVENUE': noncurrent_deferred_revenue,
            'OTHER_LT_LIABILITIES': other_lt_liabilities,
            'TOTAL_NON_CURRENT_LIABILITIES': total_non_current_liabilities,
            'TOTAL_LIABILITIES': total_liabilities,
            'COMMON_STOCK': common_stock,
            'PREFERRED_STOCK': preferred_stock,
            'RETAINED_EARNINGS': retained_earnings,
            'AOCI': aoci,
            'APIC': apic,
            'TREASURY_STOCK': treasury_stock,
            'OTHER_EQUITY': other_equity,
            'MINORITY_INTEREST_LIABILITY': minority_interest_liability,
            'TOTAL_EQUITY': total_equity,
            'TOTAL_LIABILITIES_AND_EQUITY': total_liabilities_and_equity,
            'CFO_NET_INCOME': cfo_net_income,
            'CFO_DA': cfo_da,
            'CFO_RECEIVABLES': cfo_recievables,
            'CFO_INVENTORY': cfo_inventory,
            'CFO_PREPAID_EXPENSES': cfo_prepaid_expenses,
            'CFO_OTHER_WORKING_CAPITAL': cfo_other_working_capital,
            'CFO_CHANGE_IN_WORKING_CAPITAL': cfo_change_in_working_capital,
            'CFO_DEFERRED_TAX': cfo_deferred_tax,
            'CFO_STOCK_COMP': cfo_stock_comp,
            'CFO_OTHER_NONCASH_ITEMS': cfo_other_noncash_items,
            'CF_CFO': cf_cfo,
            'CFI_PPE_PURCHASES': cfi_ppe_purchases,
            'CFI_PPE_SALES': cfi_ppe_sales,
            'CFI_PPE_NET': cfi_ppe_net,
            'CFI_ACQUISITIONS': cfi_acquistions,
            'CFI_DIVESTITURES': cfi_divestitures,
            'CFI_ACQUISITIONS_NET': cfi_acquistions_net,
            'CFI_INVESTMENT_PURCHASES': cfi_investment_purchases,
            'CFI_INVESTMENT_SALES': cfi_investment_sales,
            'CFI_INVESTMENT_NET': cfi_investment_net,
            'CFI_INTANGIBLES_NET': cfi_intangibles_net,
            'CFI_OTHER': cfi_other,
            'CF_CFI': cf_cfi,
            'CFF_COMMON_STOCK_ISSUED': cff_common_stock_issued,
            'CFF_COMMON_STOCK_REPURCHASED': cff_common_stock_repurchased,
            'CFF_COMMON_STOCK_NET': cff_common_stock_net,
            'CFF_PFD_ISSUED': cff_pfd_issued,
            'CFF_PFD_REPURCHASED': cff_pfd_repurchased,
            'CFF_PFD_NET': cff_pfd_net,
            'CFF_DEBT_ISSUED': cff_debt_issued,
            'CFF_DEBT_REPAID': cff_debt_repaid,
            'CFF_DEBT_NET': cff_debt_net,
            'CFF_DIVIDEND_PAID': cff_dividend_paid,
            'CFF_OTHER': cff_other,
            'CF_CFF': cf_cff,
            'CF_FOREX': cf_forex,
            'CF_NET_CHANGE_IN_CASH': cf_net_changes_in_cash
        })
        
        headers = [
            'REVENUE',
            'COGS',
            'GROSS_PROFIT',
            'SGA',
            'RND',
            'SPECIAL_CHARGES',
            'OTHER_OPEX',
            'TOTAL_OPEX',
            'OPERATING_INCOME',
            'INTEREST_INCOME',
            'INTEREST_EXPENSE',
            'NET_INTEREST_INCOME',
            'INCOME_TAX',
            'NET_INCOME_CONTINUING',
            'NET_INCOME_DISCONTINUED',
            'INCOME_ALLOCATED_TO_MINORITY_INTEREST',
            'OTHER_INCOME_STATEMENT_ITEMS',
            'NET_INCOME',
            'PREFERRED_DIVIDENDS',
            'NET_INCOME_AVAILABLE_TO_SHAREHOLDERS',
            'SHARES_BASIC',
            'SHARES_DILUTED',
            'EPS_BASIC',
            'EPS_DILUTED',
            'DA_INCOME_STATEMENT_SUPPLEMENTAL',
            'CASH_AND_EQUIV',
            'ST_INVESTMENTS',
            'RECEIVABLES',
            'INVENTORIES',
            'OTHER_CURRENT_ASSETS',
            'TOTAL_CURRENT_ASSETS',
            'EQUITY_AND_OTHER_INVESTMENTS',
            'PPE_GROSS',
            'ACCUMULATED_DEPRECIATION',
            'PPE_NET',
            'INTANGIBLE_ASSETS',
            'GOODWILL',
            'OTHER_LT_ASSETS',
            'TOTAL_NON_CURRENT_ASSETS',
            'TOTAL_ASSETS',
            'ACCOUNTS_PAYABLE',
            'TAX_PAYABLE',
            'CURRENT_ACCRUED_LIABILITIES',
            'ST_DEBT',
            'CURRENT_DEFERRED_REVENUE',
            'CURRENT_DEFERRED_TAX_LIABILITY',
            'CURRENT_CAPITAL_LEASES',
            'OTHER_CURRENT_LIABILITIES',
            'TOTAL_CURRENT_LIABILITIES',
            'LT_DEBT',
            'NONCURRENT_CAPITAL_LEASES',
            'PENSION_LIABILITIES',
            'NONCURRENT_DEFERRED_REVENUE',
            'OTHER_LT_LIABILITIES',
            'TOTAL_NON_CURRENT_LIABILITIES',
            'TOTAL_LIABILITIES',
            'COMMON_STOCK',
            'PREFERRED_STOCK',
            'RETAINED_EARNINGS',
            'AOCI',
            'APIC',
            'TREASURY_STOCK',
            'OTHER_EQUITY',
            'MINORITY_INTEREST_LIABILITY',
            'TOTAL_EQUITY',
            'TOTAL_LIABILITIES_AND_EQUITY',
            'CFO_NET_INCOME',
            'CFO_DA',
            'CFO_RECEIVABLES',
            'CFO_INVENTORY',
            'CFO_PREPAID_EXPENSES',
            'CFO_OTHER_WORKING_CAPITAL',
            'CFO_CHANGE_IN_WORKING_CAPITAL',
            'CFO_DEFERRED_TAX',
            'CFO_STOCK_COMP',
            'CFO_OTHER_NONCASH_ITEMS',
            'CF_CFO',
            'CFI_PPE_PURCHASES',
            'CFI_PPE_SALES',
            'CFI_PPE_NET',
            'CFI_ACQUISITIONS',
            'CFI_DIVESTITURES',
            'CFI_ACQUISITIONS_NET',
            'CFI_INVESTMENT_PURCHASES',
            'CFI_INVESTMENT_SALES',
            'CFI_INVESTMENT_NET',
            'CFI_INTANGIBLES_NET',
            'CFI_OTHER',
            'CF_CFI',
            'CFF_COMMON_STOCK_ISSUED',
            'CFF_COMMON_STOCK_REPURCHASED',
            'CFF_COMMON_STOCK_NET',
            'CFF_PFD_ISSUED',
            'CFF_PFD_REPURCHASED',
            'CFF_PFD_NET',
            'CFF_DEBT_ISSUED',
            'CFF_DEBT_REPAID',
            'CFF_DEBT_NET',
            'CFF_DIVIDEND_PAID',
            'CFF_OTHER',
            'CF_CFF',
            'CF_FOREX',
            'CF_NET_CHANGE_IN_CASH'
        ]

        ## START -> Join Data
        data_concat = pd.concat([data, data_point], axis=1) # Concat into individual columns
        data_explode = data_concat.explode(headers).reset_index(drop=True) # Explode into individual Rows
        ## END -> Join Data

        ## -----EXPLODE DATE INTO ROWS END---- ##

        print("Data Join ---",round((time_data_join-time.time())/60, 2),"mins")
        return data_explode





