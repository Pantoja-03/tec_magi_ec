import pandas as pd
import re
import magi_core.magi_connections as conn


class Validate_Phone:
    def __init__(self, mx_plan_marcacion):
        self.mx_plan_marcacion = mx_plan_marcacion
        self.mx_phone_cache = {}
        self.phone_regex = r'^([0-9])*$'
        
    def validate(self, phone):
        #Validate syntax
        is_valid_syntax = True

        if re.match(self.phone_regex, phone) != None:
            for i in range(10):
                test = str(i) * 5
                
                if phone.find(test) > -1:
                    is_valid_syntax = False
                    
            test = '12345'
            if phone.find(test) > -1:
                    is_valid_syntax = False
            
        else:
            is_valid_syntax = False

        
        if is_valid_syntax:
            if phone not in self.mx_phone_cache:
                try:               
                    ind = str(phone)[:-4]
                    current_pnm = self.mx_plan_marcacion[self.mx_plan_marcacion['IX'] == ind].reset_index()
                    
                    if len(current_pnm) > 0:
                        for ix, c in current_pnm.iterrows():
                            if (int(phone) >= int(c['START'])) and (int(phone) <= int(c['END'])):
                                self.mx_phone_cache[phone] = True                            
                                return self.mx_phone_cache[phone]
                            
                    else:
                        self.mx_phone_cache[phone] = False
                        return self.mx_phone_cache[phone]
                    
                except:
                    self.mx_phone_cache[phone] = False
                    return self.mx_phone_cache[phone]
                    
        
        return is_valid_syntax



def get_phone_to_sf(phone: str, country_code: str):
    if country_code == 'nan' or country_code == "52" or country_code == "":
        if len(phone) <= 10:
            return "52" + phone
        else:
            return phone
    elif phone[:len(country_code)] != country_code:
        return country_code + phone
    else:
        return phone



def validate(base: pd.DataFrame()):
    #GET PLAN NACIONAL DE MARCACION
    try:
        mx_plan_marcacion = pd.read_pickle('./resources/mx_plan_marcacion/pnm.pkl')
    except:
        mx_plan_marcacion = conn.get_mx_plan_marcacion()
        
    validate_phone = Validate_Phone(mx_plan_marcacion)

    #validate phone
    base['valid_phone'] = True
    base.loc[base.country_code.isin(['', '52']), 'valid_phone'] = base.loc[base.country_code.isin(['', '52']), 'phone_corrected'].apply(lambda x: validate_phone.validate(x))                                    
    base['len_phone'] = base['phone_corrected'].str.len()
    base.loc[(base.len_phone < 8) | (base.len_phone > 13), 'valid_phone'] = False


    #get phone whit country_code
    base['phone_corrected'] = base.apply(lambda row: get_phone_to_sf(str(row['phone_corrected']), str(row['country_code'])),axis=1)
    base['len_phone'] = base['phone_corrected'].str.len()
    
    #valid international phone's
    base.loc[((base.len_phone >= 8) & (base.campus.str.find('LATAM') >= 0)) | (base.phone_corrected.str[:2] != '52'), 'valid_phone'] = True
    base.loc[(base.len_phone != 12) & (base.phone_corrected.str[:2] == '52'), 'valid_phone'] = False
    
    #empty loaded_phone
    base['loaded_phone'] = base['phone_corrected'].copy()
    base.loc[base.valid_phone == False, 'loaded_phone'] = ""
    
    
    return base.copy()
    
    
    