import pandas as pd
import re
from dns import exception, resolver

class Validate_Email:
    def __init__(self, mx_dns_host):
        self.mx_dns_host = mx_dns_host
        self.email_regex = r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)'
        self.email_regex_test = r'(^[0-9.])'
        self.email_test = ['PRUEBA','TEST','EJEMPLO','EXAMPLE']
    

    def validate(self, email):
        is_valid_syntax = True

        if re.match(self.email_regex, email) != None:

            for test in self.email_test:
                letter = email[email.upper().find(test) + len(test) : email.upper().find(test) + len(test) + 1]    

                if email.upper().find(test) > -1 and re.match(self.email_regex_test, letter):
                    is_valid_syntax = False
            
        else:
            is_valid_syntax = False
        

        if is_valid_syntax:
            hostname = email[email.find('@') + 1:]

            if hostname in self.mx_dns_host:
                return True
            
            else:
                response = 0
                count = 0
                while response == 0:
                    count = count + 1
                    if count >= 10:
                        return False
                    
                    try:
                        my_resolver = resolver.Resolver()
                        my_resolver.resolve(hostname, 'MX')
                        
                        self.mx_dns_host[hostname] = True
                        return True
                        
                    except exception.DNSException as ex:
                        if str(ex).find('All nameservers failed to answer') < 0:
                            return False  
                        
        else:
            return False


def validate(base: pd.DataFrame, mx_dns_host: dict):
    #Instance
    validate_email = Validate_Email(mx_dns_host)
    
    base['valid_email'] = base['email_corrected'].apply(lambda x: validate_email.validate(x))
    
    #save current dns hosts
    mx_dns_host_new = validate_email.mx_dns_host
    mx_dns_host_new  = pd.DataFrame.from_dict(mx_dns_host_new, orient='index')
    mx_dns_host_new.to_pickle('./resources/dns_hosts.pkl')

    return base.copy()



