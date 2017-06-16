
import pkg_resources

from whoosh.support.bottle import request, route, run, SimpleTemplate


t = SimpleTemplate("""
<html>
    {{ hello }}
    % if count > 10:
        <p>Yo!</p>
    % end
</html>
""")
print(t.render(hello="Hi!", count=15))



