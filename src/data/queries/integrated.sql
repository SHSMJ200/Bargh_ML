CREATE TABLE if not exists integrated_data AS
SELECT
    c.id,
    c.name,
    c.code,
    c.date,
    c.hour,

    w.temperature,
    w.humidity,
    w.dew,
    w.apparent_temperature,
    w.precipitation,
    w.rain,
    w.snow,
    w.surface_pressure,
    w.evapotransporation,
    w.wind_speed,
    w.wind_direction,

    b.value,
    l.forecast,
    e.generation,
    s.declare,
    st.status,
    c.require

FROM commitment c

LEFT JOIN weather w 
    ON c.id = w.id AND c.date = w.date AND c.hour = w.hour

LEFT JOIN bar b 
    ON c.date = b.date AND c.hour = b.hour

LEFT JOIN load l 
    ON c.date = l.date AND c.hour = l.hour

LEFT JOIN energy e 
    ON c.id = e.id AND c.code = e.code AND c.date = e.date AND c.hour = e.hour

LEFT JOIN selleroffer s 
    ON c.id = s.id AND c.code = s.code AND c.date = s.date AND c.hour = s.hour

LEFT JOIN status st 
    ON c.id = st.id AND c.code = st.code AND c.date = st.date AND c.hour = st.hour;
