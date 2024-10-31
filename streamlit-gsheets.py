import streamlit as st
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import Optional, List, Dict, Any
import logging
import plotly.express as px
import warnings
from datetime import datetime, timedelta, date
import calendar

# Отключаем предупреждение о кэше
warnings.filterwarnings('ignore', message='file_cache is unavailable when using oauth2client >= 4.0.0')

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# Константы
NUMERIC_COLUMNS = ['руки', 'RB%_PLAYER', 'Rake_USD_PLAYER', 'RB_USD_PLAYER', 'Win_USD_PLAYER', 'Profit_PLAYER']
SPREADSHEET_ID = '1vtB1IFryFOiM13EfPFD60kQ69KaYxLQtrtB4KOsoZKo'

def get_date_ranges():
    """
    Возвращает диапазоны дат для фильтрации
    """
    today = date.today()
    first_day_current = today.replace(day=1)
    thirty_days_ago = today - timedelta(days=30)
    
    return {
        'current_month': (first_day_current, today),
        'last_30_days': (thirty_days_ago, today)
    }

@st.cache_resource(show_spinner=False, ttl=3600)  # Добавляем ttl для периодического обновления кэша
class DataLoader:
    def __init__(self):
        try:
            self.credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            self.service = build(
                "sheets", 
                "v4", 
                credentials=self.credentials,
                cache_discovery=False
            )
        except Exception as e:
            logger.error(f"Ошибка при инициализации DataLoader: {e}")
            st.error("Ошибка при инициализации подключения к Google Sheets")

    def _format_date(self, date_str: str) -> str:
        """
        Преобразует строку даты в правильный формат
        """
        try:
            # Предполагаем, что дата в формате DD.MM.YYYY
            return pd.to_datetime(date_str, format='%d.%m.%Y').date()
        except:
            try:
                # Пробуем альтернативный формат
                return pd.to_datetime(date_str).date()
            except:
                logger.error(f"Не удалось преобразовать дату: {date_str}")
                return None
    
    def load_data(self, range_name: str = "pivot_result") -> Optional[pd.DataFrame]:
        """
        Загружает данные из Google Sheets и преобразует их в DataFrame
        """
        try:
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name
            ).execute()
            values = result.get("values", [])
            
            if not values:
                st.error("Данные не найдены")
                return None
            
            # Создаем DataFrame
            df = pd.DataFrame(values[1:], columns=values[0])
            
            # Преобразуем первый столбец в дату
            date_column = df.columns[0]  # Получаем имя первого столбца
            df[date_column] = df[date_column].apply(self._format_date)
            
            return self._process_numeric_columns(df)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            st.error(f"Ошибка при загрузке данных: {str(e)}")
            return None
    
    def _process_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрабатывает числовые столбцы DataFrame и фильтрует тренеров
        """
        try:
            df = df.copy()
            
            for col in NUMERIC_COLUMNS:
                if col in df.columns:
                    df[col] = (df[col]
                              .replace('', np.nan)
                              .str.replace('\xa0', '')
                              .str.replace(',', '.')
                              .pipe(pd.to_numeric, errors='coerce'))
            
            # Фильтруем строки, где тренер = '0'
            df = df[df['Тренер'] != '0']
            return df
        except Exception as e:
            logger.error(f"Ошибка при обработке числовых столбцов: {e}")
            st.error("Ошибка при обработке данных")
            return df
            
class StreamlitApp:
    def __init__(self):
        st.set_page_config(
            page_title="Google Sheets Viewer",
            page_icon="📊",
            layout="wide"
        )
        self.data_loader = DataLoader()
        self.data = None
        self.filtered_data = None
        self.date_ranges = get_date_ranges()

    def setup_filters(self) -> Dict[str, Any]:
        """
        Настраивает фильтры данных и возвращает выбранные параметры фильтрации
        """
        with st.sidebar:
            st.header("Фильтр по датам")
            
            date_column = self.data.columns[0]  # Получаем имя первого столбца (дата)
            min_date = self.data[date_column].min()
            max_date = self.data[date_column].max()
            
            # Кнопки быстрого выбора периода
            date_filter_type = st.radio(
                "Выберите период:",
                ["Текущий месяц", "Последние 30 дней", "Все время", "Выбрать период"]
            )
            
            if date_filter_type == "Выбрать период":
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input(
                        "Начало периода",
                        value=min_date,
                        min_value=min_date,
                        max_value=max_date,
                        format="DD.MM.YYYY"
                    )
                with col2:
                    end_date = st.date_input(
                        "Конец периода",
                        value=max_date,
                        min_value=min_date,
                        max_value=max_date,
                        format="DD.MM.YYYY"
                    )
            else:
                if date_filter_type == "Текущий месяц":
                    start_date = self.date_ranges['current_month'][0]
                    end_date = self.date_ranges['current_month'][1]
                elif date_filter_type == "Последние 30 дней":
                    start_date = self.date_ranges['last_30_days'][0]
                    end_date = self.date_ranges['last_30_days'][1]
                else:  # Все время
                    start_date = min_date
                    end_date = max_date
            
            st.markdown("---")
            
            # Фильтруем данные по выбранному периоду
            period_filtered_data = self.data[
                (self.data[date_column] >= start_date) &
                (self.data[date_column] <= end_date)
            ]
            
            # Фильтр по тренеру
            all_coaches = ['Все'] + sorted(period_filtered_data['Тренер'].unique().tolist())
            selected_coach = st.selectbox(
                "Выберите тренера",
                all_coaches,
                index=0
            )
            
            # Фильтруем данные для игроков
            if selected_coach != 'Все':
                players_data = period_filtered_data[period_filtered_data['Тренер'] == selected_coach]
            else:
                players_data = period_filtered_data
                
            # Фильтр по игроку
            all_players = ['Все'] + sorted(players_data['nickname'].unique().tolist())
            selected_player = st.selectbox(
                "Выберите игрока",
                all_players,
                index=0
            )
            
            return {
                "coach": selected_coach,
                "player": selected_player,
                "grouping": ['Тренер', 'nickname'],  # Устанавливаем фиксированную группировку
                "show_percentages": False,  # Устанавливаем фиксированное значение
                "start_date": start_date,
                "end_date": end_date
            }

    def apply_filters(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Применяет фильтры к данным
        """
        filtered_data = self.data.copy()
        date_column = filtered_data.columns[0]  # Получаем имя первого столбца (дата)
        
        # Фильтрация по датам
        filtered_data = filtered_data[
            (filtered_data[date_column] >= params["start_date"]) &
            (filtered_data[date_column] <= params["end_date"])
        ]
        
        if params["coach"] != 'Все':
            filtered_data = filtered_data[filtered_data['Тренер'] == params["coach"]]
        
        if params["player"] != 'Все':
            filtered_data = filtered_data[filtered_data['nickname'] == params["player"]]
        
        return filtered_data

    def process_and_display_data(self, filtered_data: pd.DataFrame, params: Dict[str, Any]):
        """
        Обрабатывает и отображает данные
        """
        agg_dict = {
            'Profit_PLAYER': 'sum',
            'Win_USD_PLAYER': 'sum',
            'RB_USD_PLAYER': 'sum',
            'руки': 'sum',
            'Rake_USD_PLAYER': 'sum'
        }
        
        # Группировка по основным параметрам
        grouped_data = (filtered_data
                       .groupby(params["grouping"], as_index=False)
                       .agg(agg_dict)
                       .round(0)
                       .sort_values(by='Profit_PLAYER', ascending=False))
        
        # Добавляем столбец с чекбоксами
        grouped_data.insert(0, "Выбрать", False)
        
        # Создаем колонки с распределением 1:3
        col1, col2 = st.columns([4, 4])
        
        # В первой колонке отображаем таблицы
        with col1:
            st.subheader("Сгруппированные данные")
            
            # Создаем конфигурацию для столбца с чекбоксами
            config = {
                "Выбрать": st.column_config.CheckboxColumn(
                    "Выбрать",
                    help="Выберите строки для фильтрации",
                    default=False,
                )
            }
            
            # Отображаем таблицу с чекбоксами на всю ширину col1
            edited_df = st.data_editor(
                grouped_data,
                hide_index=True,
                column_config=config,
                disabled=grouped_data.columns.difference(["Выбрать"]).tolist(),
                key="data_editor",
                height=600,
                use_container_width=True
            )
            
            # Фильтруем данные на основе выбранных строк
            selected_rows = edited_df[edited_df["Выбрать"]]
            
            if not selected_rows.empty:
                filtered_for_visualization = pd.DataFrame()
                for _, row in selected_rows.iterrows():
                    mask = pd.Series(True, index=filtered_data.index)
                    for group_col in params["grouping"]:
                        mask &= filtered_data[group_col] == row[group_col]
                    filtered_for_visualization = pd.concat([filtered_for_visualization, filtered_data[mask]])
                
                filtered_grouped_data = selected_rows
            else:
                filtered_grouped_data = grouped_data
                filtered_for_visualization = filtered_data
            
            # Удаляем столбец с чекбоксами для отображения статистики
            filtered_grouped_data = filtered_grouped_data.drop(columns=["Выбрать"])
            
            # Добавляем таблицу с данными по клубам
            st.subheader("Статистика по клубам")
            
            # Группировка по клубам для отфильтрованных данных
            club_data = (filtered_for_visualization
                        .groupby('club_name', as_index=False)
                        .agg(agg_dict)
                        .round(0)
                        .sort_values(by='Profit_PLAYER', ascending=False))
            
            # Переименовываем столбцы для лучшей читаемости
            club_data = club_data.rename(columns={
                'club_name': 'Клуб',
                'Profit_PLAYER': 'Профит',
                'Win_USD_PLAYER': 'Выигрыш',
                'RB_USD_PLAYER': 'РБ',
                'руки': 'Руки',
                'Rake_USD_PLAYER': 'Рейк'
            })
            
            # Добавляем итоговую строку
            totals = club_data.sum(numeric_only=True).round(0)
            totals = pd.DataFrame([totals], columns=club_data.columns[1:])
            totals.insert(0, 'Клуб', 'ИТОГО')
            club_data = pd.concat([club_data, totals], ignore_index=True)
            
            # Отображаем таблицу с данными по клубам на всю ширину col1
            st.dataframe(
                club_data,
                hide_index=True,
                use_container_width=True,
                column_config={
                    'Клуб': st.column_config.TextColumn('Клуб'),
                    'Профит': st.column_config.NumberColumn('Профит', format="%.0f", help="Общий профит"),
                    'Выигрыш': st.column_config.NumberColumn('Выигрыш', format="%.0f", help="Общий выигрыш"),
                    'РБ': st.column_config.NumberColumn('РБ', format="%.0f", help="Общий рейкбек"),
                    'Руки': st.column_config.NumberColumn('Руки', format="%.0f", help="Количество сыгранных рук"),
                    'Рейк': st.column_config.NumberColumn('Рейк', format="%.0f", help="Общий рейк")
                }
            )
        
        # Во второй колонке отображаем итоговую статистику и график
        with col2:
            # Создаем метрики в ряд
            metric_cols = st.columns(5)
            
            # Получаем значения для метрик
            total_profit = float(filtered_grouped_data['Profit_PLAYER'].sum())
            total_win = float(filtered_grouped_data['Win_USD_PLAYER'].sum())
            total_rb = float(filtered_grouped_data['RB_USD_PLAYER'].sum())
            total_hands = float(filtered_grouped_data['руки'].sum())
            total_rake = float(filtered_grouped_data['Rake_USD_PLAYER'].sum())
            
            # Отображаем метрики
            metric_cols[0].metric("Profit", f"${total_profit:,.0f}")
            metric_cols[1].metric("Win", f"${total_win:,.0f}")
            metric_cols[2].metric("RB", f"${total_rb:,.0f}")
            metric_cols[3].metric("Руки", f"{total_hands:,.0f}")
            metric_cols[4].metric("Rake", f"${total_rake:,.0f}")
            
            # Отображаем график
            self.display_visualizations(filtered_for_visualization, params)

    def display_visualizations(self, filtered_data: pd.DataFrame, params: Dict[str, Any]):
        """
        Отображает визуализации данных
        """
        if len(filtered_data) > 1:
            date_column = filtered_data.columns[0]
            
            daily_data = filtered_data.groupby(date_column).agg({
                'Profit_PLAYER': 'sum',
                'Win_USD_PLAYER': 'sum',
                'руки': 'sum'
            }).reset_index()
            
            daily_data = daily_data.sort_values(by=date_column)
            daily_data['Profit_PLAYER_cum'] = daily_data['Profit_PLAYER'].cumsum()
            daily_data['Win_USD_PLAYER_cum'] = daily_data['Win_USD_PLAYER'].cumsum()
            
            fig = px.line(
                daily_data,
                x=date_column,
                y=['Profit_PLAYER_cum', 'Win_USD_PLAYER_cum'],
                labels={
                    'value': 'Значение',
                    'variable': 'Показатель',
                    date_column: 'Дата'
                }
            )
            
            fig.update_traces(
                line=dict(color='rgb(26,118,217)', width=4),
                name='Profit',
                selector=dict(name='Profit_PLAYER_cum')
            )
            fig.update_traces(
                line=dict(color='rgb(240,170,31)', width=2),
                name='Win',
                selector=dict(name='Win_USD_PLAYER_cum')
            )
            
            fig.add_bar(
                x=daily_data[date_column],
                y=daily_data['руки'],
                name='Руки',
                yaxis='y2',
                marker_color='rgb(140,140,140)',
                opacity=0.3
            )
            
            fig.update_layout(
                yaxis2=dict(
                    title='Количество рук',
                    overlaying='y',
                    side='right',
                    showgrid=False
                ),
                yaxis=dict(
                    title='Значение, $',
                    side='left',
                    showgrid=True
                ),
                hovermode='x unified',
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                ),
                height=600
            )
            
            st.plotly_chart(fig, use_container_width=True)

    def run(self):
        """
        Запускает приложение
        """
        st.title("Анализ данных игроков")
        
        # Загрузка данных
        self.data = self.data_loader.load_data()
        if self.data is None:
            return
        
        # Настройка фильтров и получение параметров
        params = self.setup_filters()
        
        # Применение фильтров
        filtered_data = self.apply_filters(params)
        
        # Обработка и отображение данных
        self.process_and_display_data(filtered_data, params)

if __name__ == "__main__":
    app = StreamlitApp()
    app.run()
                    
                    
