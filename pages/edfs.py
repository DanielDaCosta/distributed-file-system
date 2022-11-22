#import dash_html_components as html
# import dash_core_components as dcc
from turtle import width
import dash_bootstrap_components as dbc
from dash import dcc, html 
from dash.dependencies import Input, Output, State
from apps import navigation
import edfs.firebase as file_system

# from edfs.firebase import ls, mkdir, rm, \
#     getPartitionLocation, \
#     readPartition, \
#     put, \
#     cat, \
#     test
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
    html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="EDFS", className="menu-title"),
                        dcc.Dropdown(
                            id="edfs-filter",
                            options=['Firebase', 'MySQL', 'MongoDB'],
                            value="Firebase",
                            clearable=False,
                            placeholder="Select Indicator",
                            searchable=False,
                            className="dropdown",
                        ),
                    ]
                ),
                html.Br()
            ],
            className='menu2'
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
    ], className="output",),
    html.Div(id='dummy_edfs')
    # dcc.Link('Home',href="/"),
    # html.Br(),
    # dcc.Link('model-showcase',href="/showcase")
])

# Change EDFS
@dash.callback(
    Output("dummy_edfs", "children"),
    Input("edfs-filter", "value"),
)
def select_dataset(edfs_filter):
    global file_system
    if edfs_filter == "MySQL":
        import edfs.mysql as file_system
    elif edfs_filter == "MongoDB":
        import edfs.mongodb as file_system
    else:
        import edfs.firebase as file_system
    return None


@dash.callback(
    Output('textarea-state-example-output', 'children'),
    Input('textarea-state-example-button', 'n_clicks'),
    State('textarea-state-example', 'value')
)
def update_output(n_clicks, input_text):
    if input_text:
        functions = {
            'ls': file_system.ls, 'mkdir': file_system.mkdir, 'rm': file_system.rm,
            'getPartitionLocation': file_system.getPartitionLocation,
            'readPartition': file_system.readPartition,
            'put': file_system.put,
            'cat': file_system.cat
        }

        input = input_text.split(' ')
        function_name = input[0]
        params = input[1:]

        try:
            output = functions[function_name](*params)
        except KeyError:
            output = "Command not found. Valid commands: ls, mkdir, rm, put, getPartitionLocations, readPartition"
        except TypeError:
            output = "Please verify function required arguments"
        if n_clicks > 0:
            return '\n{}'.format(output)