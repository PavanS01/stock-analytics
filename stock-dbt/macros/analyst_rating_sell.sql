{% macro analyst_rating_sell(analystratingstrongsell, analystratingsell) -%}
    case 
        when analystratingsell=-1 or analystratingstrongsell=-1 then -1
        else analystratingsell + analystratingstrongsell
    end
{%- endmacro %}