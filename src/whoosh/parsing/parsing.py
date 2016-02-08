from grako.parser import GrakoGrammarGenerator


# s = """
# number = /[0-9]+/ ;
# """
#
# g = GrakoGrammarGenerator("whoosh").parse(s, "number")

grammar = '''
number = /[0-9]+/ ;
numbers = { number }+ ;
'''
g = GrakoGrammarGenerator('Number')
model = g.parse(grammar, trace=False)
print(repr(model.parse("9 200 300", start="numbers")))


# grammar = """
# @@whitespace :: /[\t ]+/
# # this is just a token with any character but space and newline
# # it should finish before it capture space or newline character
# token = /[^ \n]+/;
# # expect whitespace to capture spaces between tokens, but newline should be captured afterwards
# token2 = {token}* /\n/;
# # document is just list of this strings of tokens
# document = {@+:token2}* $;
# """
# text = """\
#     a b
#     c d
#     e f
# """
#
# model = GrakoGrammarGenerator("document").parse(grammar)
# ast = model.parse(text, start='document')
# print(ast)
