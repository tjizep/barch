from flask import Flask, request, render_template_string, redirect, url_for, flash
import barch

app = Flask(__name__)
app.secret_key = 'barch_demo_secret_key'  # for flash messages


# Enumerate all public data members / properties of `statistics_values`
STAT_FIELDS = [
    "bytes_allocated",
    "bytes_interior",
    "exceptions_raised",
    "heap_bytes_allocated",
    "keys_evicted",
    "last_vacuum_time",
    "leaf_nodes",
    "leaf_nodes_replaced",
    "max_page_bytes_uncompressed",
    "node4_nodes",
    "node16_nodes",
    "node48_nodes",
    "node256_nodes",
    "node256_occupants",
    "page_bytes_compressed",
    "pages_compressed",
    "pages_evicted",
    "pages_defragged",
    "pages_uncompressed",
    "vacuums_performed",
    "maintenance_cycles",
    "shards",
    "local_calls"
]

CONFIG_FIELDS = [
    "compression",
    "n_max_memory_bytes",
    "maintenance_poll_delay",
    "max_defrag_page_count",
    "save_interval",
    "max_modifications_before_save",
    "rpc_max_buffer",
    "rpc_client_max_wait_ms",
    "iteration_worker_count",
    "min_fragmentation_ratio",
    "use_vmm_memory",
    "active_defrag",
    "evict_volatile_lru",
    "evict_allkeys_lru",
    "evict_volatile_lfu",
    "evict_allkeys_lfu",
    "evict_volatile_random",
    "evict_allkeys_random",
    "evict_volatile_ttl",
    "log_page_access_trace",
    "external_host",
    "bind_interface",
    "listen_port"
]

TABLE_TEMPLATE = """
<div class="card mb-4">
    <h5 class="card-header">{{ title }}</h5>
    <div class="card-body p-0">
        <table class="table table-sm mb-0">
            <thead class="table-light"><tr><th>Field</th><th class="text-end">Value</th></tr></thead>
            <tbody>
            {% for field, value in items.items() %}
                <tr><td>{{ field }}</td><td class="text-end">{{ value }}</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
</div>
"""


STATS_TEMPLATE = """
<div class="card mb-4">
    <h5 class="card-header">Barch Statistics</h5>
    <div class="card-body p-0">
        <table class="table table-sm mb-0">
            <thead class="table-light">
            <tr><th>Field</th><th class="text-end">Value</th></tr>
            </thead>
            <tbody>
            {% for field, value in stats_dict.items() %}
                <tr>
                    <td>{{ field }}</td>
                    <td class="text-end">{{ value }}</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
</div>
"""


HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Barch Python API Demo</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body        { padding-top: 20px; }
        .container  { max-width: 800px; }
        .result-box { background:#f8f9fa;padding:15px;border-radius:5px;margin-top:15px; }
        .nav-tabs   { margin-bottom:20px; }
    </style>
</head>
<body>
<div class="container">
    <h1>Barch Python API Demo</h1>

    <ul class="nav nav-tabs">
        <li class="nav-item"><button class="nav-link {% if active_tab=='keyvalue' %}active{% endif %}"  onclick="location.href='/'">KeyValue API</button></li>
        <li class="nav-item"><button class="nav-link {% if active_tab=='orderedset' %}active{% endif %}" onclick="location.href='/orderedset'">OrderedSet API</button></li>
        <li class="nav-item"><button class="nav-link {% if active_tab=='status' %}active{% endif %}"     onclick="location.href='/status'">Database Status</button></li>
        <li class="nav-item"><button class="nav-link {% if active_tab=='statistics' %}active{% endif %}" onclick="location.href='/statistics'">Statistics</button></li>
        <li class="nav-item"><button class="nav-link {% if active_tab=='configuration' %}active{% endif %}" onclick="location.href='/configuration'">Configuration</button></li>
    </ul>

    {% if messages %}
        {% for msg in messages %}
            <div class="alert alert-info">{{ msg }}</div>
        {% endfor %}
    {% endif %}

    {{ content|safe }}

    {% if result %}
    <div class="result-box">
        <h4>Result:</h4>
        <pre class="mb-0">{{ result }}</pre>
    </div>
    {% endif %}
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
'''


# KeyValue form template
KEYVALUE_TEMPLATE = '''
<div class="card mb-4">
    <div class="card-header">
        <h3>KeyValue Operations</h3>
    </div>
    <div class="card-body">
        <form method="post" action="/keyvalue/set" class="row g-3">
            <div class="col-md-5">
                <input type="text" class="form-control" name="key" placeholder="Key" required>
            </div>
            <div class="col-md-5">
                <input type="text" class="form-control" name="value" placeholder="Value" required>
            </div>
            <div class="col-md-2">
                <button type="submit" class="btn btn-primary w-100">Set</button>
            </div>
        </form>
        
        <form method="post" action="/keyvalue/get" class="row g-3 mt-2">
            <div class="col-md-10">
                <input type="text" class="form-control" name="key" placeholder="Key" required>
            </div>
            <div class="col-md-2">
                <button type="submit" class="btn btn-success w-100">Get</button>
            </div>
        </form>
        
        <form method="post" action="/keyvalue/erase" class="row g-3 mt-2">
            <div class="col-md-10">
                <input type="text" class="form-control" name="key" placeholder="Key" required>
            </div>
            <div class="col-md-2">
                <button type="submit" class="btn btn-danger w-100">Erase</button>
            </div>
        </form>
        
        <hr>
        <h4>Sample Input</h4>
        <div class="row">
            <div class="col-md-4">
                <button onclick="populateSample('user1', 'John Doe')" class="btn btn-outline-secondary mb-2 w-100">User1: John Doe</button>
            </div>
            <div class="col-md-4">
                <button onclick="populateSample('user2', 'Jane Smith')" class="btn btn-outline-secondary mb-2 w-100">User2: Jane Smith</button>
            </div>
            <div class="col-md-4">
                <button onclick="populateSample('count', '42')" class="btn btn-outline-secondary mb-2 w-100">Count: 42</button>
            </div>
        </div>
    </div>
</div>

{% if result %}
<div class="result-box">
    <h4>Result:</h4>
    <pre>{{ result }}</pre>
</div>
{% endif %}

<script>
function populateSample(key, value) {
    const forms = document.querySelectorAll('form');
    const keyInputs = forms[0].querySelector('input[name="key"]');
    const valueInput = forms[0].querySelector('input[name="value"]');
    
    if (keyInputs && valueInput) {
        keyInputs.value = key;
        valueInput.value = value;
    }
    
    // Also update the key in other forms
    for (let i = 1; i < forms.length; i++) {
        const keyInput = forms[i].querySelector('input[name="key"]');
        if (keyInput) {
            keyInput.value = key;
        }
    }
}
</script>
'''

# OrderedSet form template
ORDEREDSET_TEMPLATE = '''
<div class="card mb-4">
    <div class="card-header">
        <h3>OrderedSet Operations</h3>
    </div>
    <div class="card-body">
        <form method="post" action="/orderedset/add" class="mb-3">
            <div class="mb-3">
                <label class="form-label">Set Name:</label>
                <input type="text" class="form-control" name="key" placeholder="Set name" required>
            </div>
            <div class="mb-3">
                <label class="form-label">Members (score and value pairs):</label>
                <div id="members-container">
                    <div class="row g-3 mb-2">
                        <div class="col-md-6">
                            <input type="text" class="form-control" name="scores[]" placeholder="Score (number)" required>
                        </div>
                        <div class="col-md-6">
                            <input type="text" class="form-control" name="values[]" placeholder="Value" required>
                        </div>
                    </div>
                </div>
                <button type="button" class="btn btn-outline-secondary mt-2" onclick="addMember()">+ Add Member</button>
            </div>
            <button type="submit" class="btn btn-primary">Add to Set</button>
        </form>
        
        <form method="post" action="/orderedset/range" class="mb-3">
            <div class="mb-3">
                <label class="form-label">Set Name:</label>
                <input type="text" class="form-control" name="key" placeholder="Set name" required>
            </div>
            <div class="row g-3">
                <div class="col-md-6">
                    <label class="form-label">Min Score:</label>
                    <input type="text" class="form-control" name="min" placeholder="Minimum score" required>
                </div>
                <div class="col-md-6">
                    <label class="form-label">Max Score:</label>
                    <input type="text" class="form-control" name="max" placeholder="Maximum score" required>
                </div>
            </div>
            <div class="form-check mt-2">
                <input class="form-check-input" type="checkbox" name="withscores" id="withscores">
                <label class="form-check-label" for="withscores">
                    Include scores in result
                </label>
            </div>
            <button type="submit" class="btn btn-success mt-3">Get Range</button>
        </form>
        
        <form method="post" action="/orderedset/card" class="mb-3">
            <div class="mb-3">
                <label class="form-label">Set Name:</label>
                <input type="text" class="form-control" name="key" placeholder="Set name" required>
            </div>
            <button type="submit" class="btn btn-info">Get Card (Count)</button>
        </form>
        
        <hr>
        <h4>Sample Input Sets</h4>
        <div class="row">
            <div class="col-md-4">
                <button onclick="populateStudentScores()" class="btn btn-outline-secondary mb-2 w-100">Student Scores</button>
            </div>
            <div class="col-md-4">
                <button onclick="populateProductPrices()" class="btn btn-outline-secondary mb-2 w-100">Product Prices</button>
            </div>
            <div class="col-md-4">
                <button onclick="populateRankings()" class="btn btn-outline-secondary mb-2 w-100">Leaderboard</button>
            </div>
        </div>
    </div>
</div>

{% if result %}
<div class="result-box">
    <h4>Result:</h4>
    <pre>{{ result }}</pre>
</div>
{% endif %}

<script>
function addMember() {
    const container = document.getElementById('members-container');
    const newRow = document.createElement('div');
    newRow.className = 'row g-3 mb-2';
    newRow.innerHTML = `
        <div class="col-md-6">
            <input type="text" class="form-control" name="scores[]" placeholder="Score (number)" required>
        </div>
        <div class="col-md-6">
            <input type="text" class="form-control" name="values[]" placeholder="Value" required>
        </div>
    `;
    container.appendChild(newRow);
}

function populateStudentScores() {
    // Clear existing members
    document.getElementById('members-container').innerHTML = '';
    
    // Set name
    document.querySelector('input[name="key"]').value = 'students';
    
    // Add student scores
    const scores = [95, 87, 92, 78, 100];
    const names = ['Alice', 'Bob', 'Charlie', 'David', 'Eva'];
    
    for (let i = 0; i < scores.length; i++) {
        addMember();
    }
    
    // Fill in the values
    const scoreInputs = document.querySelectorAll('input[name="scores[]"]');
    const valueInputs = document.querySelectorAll('input[name="values[]"]');
    
    for (let i = 0; i < scores.length; i++) {
        scoreInputs[i].value = scores[i];
        valueInputs[i].value = names[i];
    }
}

function populateProductPrices() {
    // Clear existing members
    document.getElementById('members-container').innerHTML = '';
    
    // Set name
    document.querySelector('input[name="key"]').value = 'products';
    
    // Add product prices
    const prices = [10.99, 24.50, 5.75, 199.99, 0.99];
    const products = ['T-shirt', 'Jeans', 'Socks', 'Sneakers', 'Pen'];
    
    for (let i = 0; i < prices.length; i++) {
        addMember();
    }
    
    // Fill in the values
    const scoreInputs = document.querySelectorAll('input[name="scores[]"]');
    const valueInputs = document.querySelectorAll('input[name="values[]"]');
    
    for (let i = 0; i < prices.length; i++) {
        scoreInputs[i].value = prices[i];
        valueInputs[i].value = products[i];
    }
}

function populateRankings() {
    // Clear existing members
    document.getElementById('members-container').innerHTML = '';
    
    // Set name
    document.querySelector('input[name="key"]').value = 'leaderboard';
    
    // Add leaderboard rankings
    const ranks = [1, 2, 3, 4, 5];
    const players = ['Champion', 'Runner-up', 'ThirdPlace', 'FourthPlace', 'FifthPlace'];
    
    for (let i = 0; i < ranks.length; i++) {
        addMember();
    }
    
    // Fill in the values
    const scoreInputs = document.querySelectorAll('input[name="scores[]"]');
    const valueInputs = document.querySelectorAll('input[name="values[]"]');
    
    for (let i = 0; i < ranks.length; i++) {
        scoreInputs[i].value = ranks[i];
        valueInputs[i].value = players[i];
    }
}
</script>
'''

# Status template
STATUS_TEMPLATE = '''
<div class="card">
    <div class="card-header">
        <h3>Database Status</h3>
    </div>
    <div class="card-body">
        <p>Current database size: <strong>{{ size }}</strong> keys</p>
        
        <div class="d-flex gap-2 mt-3">
            <form method="post" action="/clear">
                <button type="submit" class="btn btn-danger">Clear Database</button>
            </form>
            
            <form method="post" action="/save">
                <button type="submit" class="btn btn-success">Save Database</button>
            </form>
        </div>
    </div>
</div>
'''

# Routes
def build_items(obj, fields):
    """Return dict(field -> value|n/a) for the given object."""
    return {f: getattr(obj, f, "n/a") for f in fields}

def render_table(title, items):
    return render_template_string(TABLE_TEMPLATE, title=title, items=items)


# Routes
@app.route('/')
def index():
    result    = request.args.get('result')
    messages  = request.args.getlist('message')

    # First render the inner KeyValue template so its Jinja tags are evaluated
    content_html = render_template_string(KEYVALUE_TEMPLATE, result=result)

    return render_template_string(
        HTML_TEMPLATE,
        content=content_html,          # already-rendered HTML
        active_tab='keyvalue',
        messages=messages
    )

@app.route('/statistics')
def statistics():
    messages = request.args.getlist("message")
    try:
        items = build_items(barch.stats(), STAT_FIELDS)
    except Exception as exc:
        items = {"error": f"Unable to fetch statistics: {exc}"}

    content_html = render_table("Barch Statistics", items)
    return render_template_string(
        HTML_TEMPLATE,
        content=content_html,
        active_tab="statistics",
        messages=messages
    )

@app.route('/configuration')
def configuration():
    """Show configuration values exposed by the C++ `configuration_values` object."""
    messages = request.args.getlist("message")
    try:
        items = build_items(barch.config(), CONFIG_FIELDS)
    except Exception as exc:
        items = {"error": f"Unable to fetch configuration: {exc}"}

    content_html = render_table("Barch Configuration", items)
    return render_template_string(
        HTML_TEMPLATE,
        content=content_html,
        active_tab="configuration",
        messages=messages
    )

@app.route('/orderedset')
def orderedset():
    result    = request.args.get('result')
    messages  = request.args.getlist('message')

    # First render the inner OrderedSet template so its Jinja tags are evaluated
    content_html = render_template_string(ORDEREDSET_TEMPLATE, result=result)

    return render_template_string(
        HTML_TEMPLATE,
        content=content_html,          # already-rendered HTML
        active_tab='orderedset',
        messages=messages
    )

# status route is unchanged

@app.route('/status')
def status():
    size = barch.size()
    return render_template_string(
        HTML_TEMPLATE,
        content=STATUS_TEMPLATE.replace('{{ size }}', str(size)),
        active_tab='status',
        messages=request.args.getlist('message')
    )

# KeyValue operations
@app.route('/keyvalue/set', methods=['POST'])
def keyvalue_set():
    key = request.form.get('key')
    value = request.form.get('value')

    k = barch.KeyValue()
    k.set(key, value)

    return redirect(url_for('index', message=f'Successfully set {key} = {value}'))

@app.route('/keyvalue/get', methods=['POST'])
def keyvalue_get():
    key = request.form.get('key')

    k = barch.KeyValue()
    try:
        value = k.get(key)
        result = f'Value for key "{key}": {value}'
    except Exception as e:
        result = f'Error: {str(e)}'

    return redirect(url_for('index', result=result))

@app.route('/keyvalue/erase', methods=['POST'])
def keyvalue_erase():
    key = request.form.get('key')

    k = barch.KeyValue()
    k.erase(key)

    return redirect(url_for('index', message=f'Successfully erased key: {key}'))

# OrderedSet operations
@app.route('/orderedset/add', methods=['POST'])
def orderedset_add():
    key = request.form.get('key')
    scores = request.form.getlist('scores[]')
    values = request.form.getlist('values[]')

    # Format for the add method: [score1, value1, score2, value2, ...]
    members = []
    for i in range(len(scores)):
        members.append(scores[i])
        members.append(values[i])

    z = barch.OrderedSet()
    result = z.add(key, [], members)
    count = result.i()

    return redirect(url_for('orderedset', message=f'Added {count} members to set {key}'))

@app.route('/orderedset/range', methods=['POST'])
def orderedset_range():
    key = request.form.get('key')
    min_score = float(request.form.get('min'))
    max_score = float(request.form.get('max'))
    with_scores = 'withscores' in request.form

    z = barch.OrderedSet()

    options = ["WITHSCORES"] if with_scores else []

    try:
        results = z.range(key, min_score, max_score, options)

        # Format results
        formatted_results = []
        i = 0
        while i < len(results):
            if with_scores:
                if i + 1 < len(results):
                    formatted_results.append(f"{results[i].s()} (score: {results[i+1].d()})")
                    i += 2
                else:
                    formatted_results.append(results[i].s())
                    i += 1
            else:
                formatted_results.append(results[i].s())
                i += 1

        result = "Results:\n" + "\n".join(formatted_results)
    except Exception as e:
        result = f'Error: {str(e)}'

    return redirect(url_for('orderedset', result=result))

@app.route('/orderedset/card', methods=['POST'])
def orderedset_card():
    key = request.form.get('key')

    z = barch.OrderedSet()
    try:
        card = z.card(key).i()
        result = f'Set {key} has {card} members'
    except Exception as e:
        result = f'Error: {str(e)}'

    return redirect(url_for('orderedset', result=result))

# Database operations
@app.route('/clear', methods=['POST'])
def clear_db():
    barch.clear()
    return redirect(url_for('status', message='Database cleared successfully'))

@app.route('/save', methods=['POST'])
def save_db():
    barch.save()
    return redirect(url_for('status', message='Database saved successfully'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)