from dataclasses import dataclass


@dataclass(frozen=True)
class DomainPolicy:
    domain: str
    primary_source: str
    fallback_sources: tuple[str, ...]


POLICIES = {
    'quotes': DomainPolicy(domain='quotes', primary_source='pytdx', fallback_sources=('tushare',)),
    'realtime': DomainPolicy(domain='realtime', primary_source='pytdx', fallback_sources=('tushare',)),
    'daily_bars': DomainPolicy(domain='daily_bars', primary_source='pytdx', fallback_sources=('tushare',)),
    'minute_bars': DomainPolicy(domain='minute_bars', primary_source='pytdx', fallback_sources=('baostock',)),
    'financial_statements': DomainPolicy(domain='financial_statements', primary_source='tushare', fallback_sources=()),
    'corporate_actions': DomainPolicy(domain='corporate_actions', primary_source='pytdx', fallback_sources=('tushare',)),
}


def get_domain_policy(domain: str) -> DomainPolicy:
    return POLICIES[domain]
