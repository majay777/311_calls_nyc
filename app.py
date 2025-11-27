import plotly.io as pio
from sqlalchemy import create_engine

pio.templates.default = "plotly"
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, callback
from dash import html
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv

load_dotenv()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

user_name = os.getenv("DAGSTER_PG_USERNAME")
db_pwd = os.getenv("DAGSTER_PG_PASSWORD")
hostname = os.getenv("DAGSTER_PG_HOST")
dbname = os.getenv("DAGSTER_PG_DB")
port = os.getenv("POSTGRES_PORT")

engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(user_name, db_pwd, hostname, port, dbname))



# Now use the engine with pandas

combined_df = pd.read_sql_query("SELECT * FROM call_records", engine)


combined_df['created_date'] = pd.to_datetime(combined_df['created_date'].astype(str).str.strip(), format='ISO8601')
combined_df['month'] = combined_df['created_date'].dt.month
combined_df['day'] = combined_df['created_date'].dt.day_name()
combined_df['hour'] = combined_df['created_date'].dt.hour
combined_df['year'] = combined_df['created_date'].dt.year

df = combined_df.groupby(['month', 'agency_name', 'borough'])['unique_key'].count()
df1 = combined_df.groupby(['year', 'agency_name', 'borough'])['unique_key'].count()
df1 = df1.to_frame().reset_index()
df = df.to_frame().reset_index()
df_app2 = combined_df.groupby(['incident_zip', 'complaint_type'])['unique_key'].count().to_frame().reset_index()
available_agency = df['agency_name'].unique()

app.layout = dbc.Container([
    dbc.Row(
        [dbc.Col(
            dcc.DatePickerRange(
                id='date-picker-range', start_date=combined_df['created_date'].min(),
                end_date=combined_df['created_date'].max(),
                display_format='YYYY-MM-DD'

            ),
        ),

            dbc.Col(
                html.P("Agency: "), width=1),
            dbc.Col(

                dcc.Dropdown(
                    id='agency',
                    options=[{'label': i, 'value': i} for i in available_agency],
                    value='New York City Police Department',

                ), width=6,
            ),
            dbc.Col(dcc.Graph(id='agency_month'), width=6),
            dbc.Col(dcc.Graph(id='pie_chart'), width=6)
        ]
    ),
    dbc.Row(
        [
            html.Hr(),
            dbc.Row(
                html.P("Year: ")),
            dbc.Row(

                dcc.Dropdown(
                    id='year',
                    options=[{'label': i, 'value': i} for i in df1['year'].unique()],
                    value='2023',

                ),
            ),
            dbc.Col(dcc.Graph(id='Complaints_Borough'), width=8),
            dbc.Col(html.Div(
                [

                    dcc.Graph(id='Complaint_types')]), width=4)

        ]
    ),
    dbc.Row([
        html.Label('Open vs. Close Cases By Agency'),
        dbc.Col(
            dcc.Graph(id='Agency_status')
        )
    ]),
    dbc.Row([
        dbc.Col(
            html.Div([
                html.H4("Locations(zip)"),
                dcc.Dropdown(
                    id='zip_bar',
                    options=[{'label': i, 'value': i} for i in df_app2['incident_zip'].unique()],
                    value=10000,

                ),
                dcc.Graph(id='zip_bar_graph')
            ])

        )
    ]),
    dbc.Row(
        [
            dbc.Col(
                html.Div(
                    [
                        html.H4('Calls by hour'),
                        dcc.Dropdown(id='year2',
                                     options=[{'label': i, 'value': i} for i in combined_df['year'].unique()],
                                     value='2022'),
                        dcc.Graph(id='graph_calls')
                    ]
                )
            ),
            dbc.Col(
                html.Div(
                    [
                        html.Br(),
                        html.H4("Calls by day"),
                        dcc.Graph(id='graph_day'),
                    ]
                )
            )
        ]
    )

])


@callback(
    [Output('agency_month', component_property='figure'),
     Output('pie_chart', component_property='figure')
     ],
    [Input('agency', 'value'),
     Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date')])
def update_fig(value, start_date, end_date):
    dff = combined_df[(combined_df['agency_name'] == value) & (combined_df['created_date'] >= start_date) &
                      (combined_df['created_date'] <= end_date)]
    dff = dff.groupby(['month', 'agency_name', 'borough'])['unique_key'].count()
    dff = dff.to_frame().reset_index()

    fig = px.bar(dff, x='borough', y='unique_key', color='month', text_auto='.2s',
                 labels={'unique_key': 'Number of Complaints'}, title='Monthly Calls per Borough')

    fig2 = go.Figure(data=[go.Pie(labels=dff[['borough']], values=dff['unique_key'], textinfo='label+percent',
                                  insidetextorientation='horizontal', pull=[0, 0, 0, 0, 0.3, 0.1],
                                  textposition='inside',
                                  hole=0.1, title='Complaints by borough'
                                  )])
    return fig, fig2


@callback(Output('Agency_status', 'figure'),
          Input('year', 'value'))
def update_status_graph(value):
    dff1 = combined_df[combined_df['year'] == value]
    op_cl_df = dff1.groupby(['agency', 'status'])['unique_key'].count()
    op_cl_df = op_cl_df.to_frame().reset_index()
    op_cl_df = op_cl_df.sort_values(by='unique_key', ascending=False)
    figure1 = px.bar(op_cl_df, x='unique_key', y='agency',
                     color='status',
                     labels={'agency': 'Agency', 'unique_key': 'Number of Cases'}, height=400)
    return figure1


@callback(
    Output('Complaints_Borough', 'figure'),
    Input('year', 'value'))
def update_fig3(value):
    dff1 = df1[df1['year'] == value]
    fig3 = make_subplots(rows=3, cols=2, specs=[[{'type': 'domain'}, {'type': 'domain'}]
        , [{'type': 'domain'}, {'type': 'domain'}], [{'type': 'domain'}, {'type': 'domain'}]
                                                ],
                         subplot_titles=['BROOKLYN', 'QUEENS', 'MANHATTAN', 'BRONX', 'STATEN ISLAND', 'Unspecified'])
    df_brooklyn = dff1[dff1['borough'] == 'BROOKLYN']
    df_Queens = dff1[dff1['borough'] == 'QUEENS']
    df_Manhattan = dff1[dff1['borough'] == 'MANHATTAN']
    df_Bronx = dff1[dff1['borough'] == 'BRONX']
    df_staten_island = dff1[dff1['borough'] == 'STATEN ISLAND']
    df_unspecified = dff1[dff1['borough'] == 'Unspecified']
    fig3.add_trace(go.Pie(labels=df_brooklyn['agency_name'].unique(), values=df_brooklyn['unique_key'], name="Brooklyn",
                          textposition='inside', ),
                   1, 1)
    fig3.add_trace(go.Pie(labels=df_Queens['agency_name'].unique(), values=df_Queens['unique_key'], name="Queens",
                          textposition='inside', ),
                   1, 2)
    fig3.add_trace(
        go.Pie(labels=df_Manhattan['agency_name'].unique(), values=df_Manhattan['unique_key'], name="Manhattan",
               textposition='inside', ),
        2, 1)
    fig3.add_trace(go.Pie(labels=df_Bronx['agency_name'].unique(), values=df_Bronx['unique_key'], name="Bronx",
                          textposition='inside', ),
                   2, 2)
    fig3.add_trace(
        go.Pie(labels=df_staten_island['agency_name'].unique(), values=df_staten_island['unique_key'],
               name="Staten Island", textposition='inside', ),
        3, 1)
    fig3.add_trace(
        go.Pie(labels=df_unspecified['agency_name'].unique(), values=df_unspecified['unique_key'], name="Unspecified",
               textposition='inside', ),
        3, 2)
    fig3.update_traces(hole=.6, hoverinfo="label+percent+name")
    fig3.update_layout(title_text="Complaints by borough", )
    return fig3


@callback(Output('Complaint_types', 'figure'),
          Input('year', 'value'))
def update_complaint_types(value):
    df_pie = combined_df[combined_df['year'] == value]
    df_pie = df_pie.groupby(['complaint_type'])['unique_key'].count()
    df_pie = df_pie.to_frame().reset_index()
    df_pie = df_pie.sort_values('unique_key', ascending=False).head(3)
    figure = go.Figure(data=[
        go.Pie(labels=df_pie['complaint_type'], values=df_pie['unique_key'], hole=0.3)])
    figure.update_layout(title_text="Top 3 Complaint Types")
    return figure


@callback(Output('zip_bar_graph', 'figure'),
          Input('zip_bar', 'value'))
def update_zip_bar(value):
    
    df_zip_bar = df_app2[df_app2['incident_zip'] == value]
    fig_zip_bar = px.bar(df_zip_bar, x='complaint_type', y='unique_key', text_auto='0.2s',
                         labels={'unique_key': 'Number of Complaints'}, title='Calls by Zip Address')
    return fig_zip_bar


@callback([Output('graph_calls', 'figure'),
           Output('graph_day', 'figure')],
          Input('year2', 'value'))
def update_calls(value):
    df2 = combined_df[combined_df['year'] == value].groupby(['hour'])['unique_key'].count().to_frame().reset_index()
    fig2 = px.bar(df2, x='hour', y='unique_key', title='Complaints by Hour',
                  labels={'unique_key': 'Number of Complaints'}, text_auto='0.2s')

    df3 = combined_df[combined_df['year'] == value].groupby(['day'])['unique_key'].count().to_frame().reset_index()
    fig3 = px.bar(df3, x='day', y='unique_key', title='Complaints By Day', color='day',
                  labels={'unique_key': 'Number Of Complaints'}, text_auto='0.1s')

    return fig2, fig3


if __name__ == '__main__':
    app.run_server(debug=True)
