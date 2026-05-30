import os
import sys

sys.path.insert(0, '/app/src')
os.chdir('/app/src/web')

from app import app

port = int(os.environ.get('PORT', 8080))
print(f'[START] binding on 0.0.0.0:{port}', flush=True)
app.run(host='0.0.0.0', port=port, debug=False, threaded=True, use_reloader=False)
