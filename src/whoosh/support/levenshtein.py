"""
Contains functions implementing the Levenshtein distance algorithm.
"""

def relative(a, b):
    """Returns the relative distance between two strings, in the range
    [0-1] where 1 means total equality.
    """
    d = distance(a,b)
    longer = float(max((len(a), len(b))))
    shorter = float(min((len(a), len(b))))    
    r = ((longer - d) / longer) * (shorter / longer)
    return r

def distance(s, t):
    """Returns the Levenshtein edit distance between two strings."""
    
    m, n = len(s), len(t)
    d = [range(n+1)]
    d += [[i] for i in range(1,m+1)]
    for i in range(0,m):
        for j in range(0,n):
            cost = 1
            if s[i] == t[j]: cost = 0
            d[i+1].append(min(d[i][j+1]+1,  # deletion
                              d[i+1][j]+1,  # insertion
                              d[i][j]+cost) # substitution
                         )
    return d[m][n]

