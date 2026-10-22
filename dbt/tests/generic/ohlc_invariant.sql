{# Every bar must satisfy low <= open, close <= high and low <= high. #}
{% test ohlc_invariant(model) %}
select *
from {{ model }}
where not (
    low <= open and open <= high
    and low <= close and close <= high
    and low <= high
)
{% endtest %}
