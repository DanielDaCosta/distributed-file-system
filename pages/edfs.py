#import dash_html_components as html
# import dash_core_components as dcc
from turtle import width
import dash_bootstrap_components as dbc
from dash import dcc, html 
from dash.dependencies import Input, Output, State
from apps import navigation
import dash

dash.register_page(
    __name__,
    path='/edfs'
)

layout = html.Div(children=[
    navigation.navbar,
    html.Div(
        children=[
            # html.P(children="🥑", className="header-emoji"),
            html.H1(
                children="EDFS Commands", className="header-title"
            ),
            html.P(
                children="Interface to support the EDFS commands",
                className="header-description",
            ),
        ],
        className="header",
    ),
    html.Div([
        # dcc.Textarea(
        #     id='textarea-state-example',
        #     value='Textarea content initialized\nwith multiple lines of text',
        #     style={'width': '80%', 'height': 50},
        # ),
        html.Div(id='empty-div', style={'whiteSpace': 'pre-line'}),
        dbc.Input(
            id='textarea-state-example', placeholder="Valid input...", valid=True, className="mb-3",
        ),
        # html.Button('Submit', id='textarea-state-example-button', n_clicks=0),
        dbc.Button(
            "Run Command",
            id='textarea-state-example-button',
            # outline=True,
            class_name="me-2",
            n_clicks=0
        )],
        className="d-grid gap-2 col-3 mx-auto"
    ),
    html.Div([
        dbc.Row([
            dbc.Col(html.Div()),
            dbc.Col(
                html.Div(
                    id='textarea-state-example-output',
                    style={'whiteSpace': 'pre-line'})
            ),
            dbc.Col(html.Div()),
        ],
        align="center")
    ], className="output",)
    # dcc.Link('Home',href="/"),
    # html.Br(),
    # dcc.Link('model-showcase',href="/showcase")
])

@dash.callback(
    Output('textarea-state-example-output', 'children'),
    Input('textarea-state-example-button', 'n_clicks'),
    State('textarea-state-example', 'value')
)
def update_output(n_clicks, value):
    value="""
{
    "TableCount": "3",
    "Tables": [
        {
            "TableName": "test_rds",
            "TablePath": "TEST_RDS/",
            "TableOwner": "dbo",
            "TableColumns": [
                {
                    "ColumnName": "id",
                    "ColumnType": "INT8",
                    "ColumnNullable": "false",
                    "ColumnIsPk": "true"
                },
                {
                    "ColumnName": "name",
                    "ColumnType": "STRING",
                    "ColumnLength": "30"
                },
                {
                    "ColumnName": "cel",
                    "ColumnType": "STRING",
                    "ColumnLength": "30"
                }
            ],
            "TableColumnsTotal": "3"
        },
    """
    if n_clicks > 0:
        return 'You have entered: \n{}'.format(value)