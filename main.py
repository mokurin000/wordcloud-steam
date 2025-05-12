from functools import partial
from typing import Callable
from collections import Counter
from pathlib import Path

import jieba
import polars as pl
from xdialog import open_file

from pyecharts.charts import WordCloud
from pyecharts.globals import SymbolType


# split words, then join them using a single space.
def split_words(stopwords: list[str], sentence: str) -> str:
    return " ".join(
        word.lower()
        for word in jieba.cut(sentence)
        if word not in stopwords and not word.isascii() and word.isalpha()
    )


def join_strings(
    splitor: Callable[[str], str], df: pl.DataFrame, suggest_type: str
) -> str:
    return (
        df.filter(pl.col("是否推荐/好评").eq(suggest_type))
        .with_columns(pl.col("内容").map_elements(splitor, return_dtype=pl.String))
        .get_column("内容")
        .str.join(" ")
    )[0]


def main():
    excel = Path(open_file("请选择要处理的表格", (("Excel", "*.xlsx"),)))
    df = pl.read_excel(
        excel, schema_overrides={"游戏时长(小时)": pl.Float64, "产品拥有数": pl.Int32}
    )

    with open("data/stopwords.txt", encoding="utf-8") as f:
        stopwords = f.read().split()
    split_word: Callable[[str], str] = partial(split_words, stopwords)

    for suggest_type in ["推荐", "不推荐"]:
        words = join_strings(split_word, df, suggest_type).split()
        counter = Counter(words)

        output_path = excel.with_name(
            excel.name.replace(".xlsx", f"_词云_{suggest_type}.html")
        )

        WordCloud().add(
            "",
            counter.items(),
            shape=SymbolType.DIAMOND,
            word_size_range=[20, 80],
        ).render(output_path)

    point_output_path = excel.with_name(excel.name.replace(".xlsx", "_散点图.html"))
    df.plot.point(x="产品拥有数", y="游戏时长(小时)", color="是否推荐/好评").save(
        point_output_path, format="html"
    )


if __name__ == "__main__":
    main()
