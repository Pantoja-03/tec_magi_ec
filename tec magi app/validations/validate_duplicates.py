import pandas as pd
import magi_core.magi_connections as conn


def get_sf_leads():
    #Get salesforce leads 
    try:
        sf_leads = conn.get_sf_leads()
    except:
        sf_leads = pd.read_pickle('./resources/catalogos/sf_leads.pkl')

    return sf_leads.copy()


def validate(base : pd.DataFrame):
    sf_leads = get_sf_leads()

    #Get key
    base['KEY'] = base.apply(lambda row: str(row['email_corrected']).upper().strip() + str(row['VEC_Programa__c']).upper().strip(),axis=1)
    
    #duplicates in base
    base['duplicated'] = base.duplicated(subset=['KEY'])
    base['repeat_in_base'] = base.apply(lambda row: list(base['KEY']).count(row['KEY']),axis=1)

    #duplicates in sf
    base['duplicated_sf'] = base['KEY'].isin(sf_leads['KEY'])
    base['recidivist'] = base['email_corrected'].isin(sf_leads['Email'])
    
    sf_ids = sf_leads[['KEY','Id',]].drop_duplicates('KEY').set_index('KEY')

    #Get sf id duplicates
    base['sf_id'] = base['KEY'].map(sf_ids['Id'])

    del base['KEY']
    
    return base.copy(), sf_leads.copy()