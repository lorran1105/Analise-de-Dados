{% macro classificar_renda(coluna_renda) %}
    case
        when {{ coluna_renda }} >= 13846 then 'Renda Alta'
        when {{ coluna_renda }} >= 4466 then 'Renda Media-Alta'
        when {{ coluna_renda }} >= 1136 then 'Renda Media-Baixa'
        else 'Renda Baixa'
    end
{% endmacro %}

{% macro classificar_inflacao(coluna_inflacao) %}
    case
        when {{ coluna_inflacao }} >= 20 then 'Hiperinflacao'
        when {{ coluna_inflacao }} >= 5 then 'Alta Inflacao'
        when {{ coluna_inflacao }} >= 0 then 'Inflacao Moderada'
        else 'Deflacao/Estagnacao'
    end
{% endmacro %}

{% macro classificar_crescimento_pib(coluna_crescimento_pib) %}
    case
        when {{ coluna_crescimento_pib }} < 0 then 'Recessao'
        when {{ coluna_crescimento_pib }} >= 5 then 'Forte Crescimento'
        when {{ coluna_crescimento_pib }} >= 2 then 'Crescimento Moderado'
        else 'Estagnacao Economica'
    end
{% endmacro %}

{% macro classificar_nivel_violencia(coluna_homicidios) %}
    case
        when {{ coluna_homicidios }} >= 20 then 'Muito Violento'
        when {{ coluna_homicidios }} >= 10 then 'Violento'
        when {{ coluna_homicidios }} >= 5 then 'Moderado'
        when {{ coluna_homicidios }} > 0 then 'Pouco Violento'
        else 'Sem Violencia Registrada'
    end
{% endmacro %}

{% macro classificar_cobertura_florestal(coluna_florestas) %}
    case
        when {{ coluna_florestas }} >= 50 then 'Muita Floresta'
        when {{ coluna_florestas }} >= 30 then 'Floresta Consideravel'
        when {{ coluna_florestas }} >= 10 then 'Pouca Floresta'
        else 'Muito Pouca Floresta'
    end
{% endmacro %}

{% macro classificar_expectativa_vida(coluna_expectativa) %}
    case
        when {{ coluna_expectativa }} >= 80 then 'Alta Expectativa de Vida'
        when {{ coluna_expectativa }} >= 70 then 'Media Expectativa de Vida'
        else 'Baixa Expectativa de Vida'
    end
{% endmacro %}

{% macro classificar_densidade_populacional(coluna_populacao, coluna_area) %}
    -- Classificação de densidade populacional (baseada na população por km²)
    case
        when ({{ coluna_populacao }} / {{ coluna_area }}) > 500 then 'Populacao Densa'
        when ({{ coluna_populacao }} / {{ coluna_area }}) > 100 then 'Populacao Moderada'
        when ({{ coluna_populacao }} / {{ coluna_area }}) > 10 then 'Populacao Baixa'
        else 'Populacao Muito Baixa'
    end
{% endmacro %}

{% macro classificar_tamanho_pais(coluna_area) %}
    -- Classificação de tamanho do país (baseada na área em km²)
    case
        when {{ coluna_area }} > 2000000 then 'Muito Grande'
        when {{ coluna_area }} > 200000 then 'Grande'
        when {{ coluna_area }} > 20000 then 'Medio'
        else 'Pequeno'
    end
{% endmacro %}