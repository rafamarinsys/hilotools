from pipeline.utils import parse_decimal_series
import pandas as pd

def test_parse_decimal_series():
    s = pd.Series(["1.234,56", "1,234.56", "1234,56", "1 234,56", None, "abc", 12.5, 1000])
    out = parse_decimal_series(s)
    assert round(out.iloc[0],2) == 1234.56
    assert round(out.iloc[1],2) == 1234.56
    assert round(out.iloc[2],2) == 1234.56
    assert round(out.iloc[3],2) == 1234.56
    assert pd.isna(out.iloc[4])
    assert pd.isna(out.iloc[5])
    assert out.iloc[6] == 12.5
    assert out.iloc[7] == 1000.0