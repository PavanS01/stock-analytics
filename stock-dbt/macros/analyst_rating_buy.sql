{% macro analyst_rating_buy(analystratingstrongbuy, analystratingbuy) -%}
    case 
        when analystratingbuy=-1 or analystratingstrongbuy=-1 then -1
        else analystratingbuy + analystratingstrongbuy
    end
{%- endmacro %}