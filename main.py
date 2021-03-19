from flask import Flask, render_template, request

from db import get_list_of_real_estate, get_detail_of_real_estate, get_list_of_metro

app = Flask(__name__)


@app.route('/', methods=("GET", "POST"))
def index():
    filter = {}

    if request.method == "POST":
        print(request.form)
        try:
            metro = request.form.get("metro")
            if metro > "-1":
                filter["metro"] = metro
        except:
            pass

        try:
            filter["floor_min"] = request.form.get("floor_min")
        except:
            pass

        try:
            filter["floor_max"] = request.form.get("floor_max")
        except:
            pass

        try:
            filter["square_min"] = request.form.get("square_min")
        except:
            pass

        try:
            filter["square_max"] = request.form.get("square_max")
        except:
            pass

    metro = get_list_of_metro()
    model = get_list_of_real_estate(filter)

    return render_template('index.html', model=model, metro=metro)


@app.route('/detail/<id>')
def detail(id):
    model = get_detail_of_real_estate(id)
    return render_template('detail.html', model=model)


if __name__ == "__main__":
    app.run(debug=True)
