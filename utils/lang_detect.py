import re


class LangDetector(object):
    """
    检测语言，传入字符串初始化，也可以不传字符串
    """

    # 不能检测出语言的字符
    # 先清理这些字符，再用正则判断unicode判断语言
    remove_characters = """[’·°–!"#$%&\'()*+,-./:;<=>?@，。?★、
	…【】（）《》？“”‘’！[\\]^_`{|}~0-9]+"""
    lang_patternlist = {
        "zh": re.compile("[\u4e00-\u9fff]"),
        "en": re.compile("[a-zA-Z]"),
    }
    default_lang = "en"

    def detect(
        self,
        text=None,
        cleaning=True,
        specific=False,
        cleaningExclude=False,
        unknownUseDefault=True,
    ):
        if cleaning:
            if cleaningExclude:
                text = self.cleaning_exclude(text)
            else:
                text = self.cleaning_text(text)
        resDict = {}
        for lang in self.lang_patternlist.keys():
            resDict[lang] = 0
        unknown_list = []
        for c in text:
            match_flag = False
            for lang, pattern in self.lang_patternlist.items():
                if pattern.match(c):
                    match_flag = True
                    resDict[lang] += 1
            if not match_flag:
                unknown_list.append(c)

        if unknownUseDefault:
            for c in unknown_list:
                resDict[self.default_lang] += 1
            unknown_list.clear()
        result_list = []
        for lang, count in resDict.items():
            if count > 0:
                result_list.append([lang, count / len(text)])
        if unknown_num := len(unknown_list) > 0:
            result_list.append(["unknown", unknown_num / len(text)])
        self.sort_lang_list(result_list)
        if specific:
            return result_list[0][0]
        return result_list

    def cleaning_text(self, text):
        return re.sub(self.remove_characters, "", text)

    def cleaning_exclude(self, text):
        patternStr = self.exclude_lang_pattern()
        return re.sub(patternStr, "", text)

    def sort_lang_list(self, lang_list):
        """
        排序的字符串的格式是[['zh', 0.35714285714285715], ['ja', 0.6428571428571429]]
        """

        def sort_key(x):
            return x[1]

        lang_list.sort(key=sort_key, reverse=True)

    def exclude_lang_pattern(self):
        concatStr = ""
        for pattern in self.lang_patternlist.values():
            concatStr += pattern.pattern
        tempstr = concatStr.replace("[", "").replace("]", "")
        return f"[^{tempstr}]"


def detect_language(text: str):
    lang = LangDetector().detect("\n".join(text))
    if len(lang) > 0 and (
        lang[0][0] == "zh"
        or (len(lang) > 1 and lang[1][0] == "zh" and lang[1][1] > 0.2)
    ):
        language = "Chinese"
    else:
        language = "English"
    return language
