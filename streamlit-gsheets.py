import streamlit as st
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import Optional, List, Dict, Any, Tuple
import logging
import plotly.express as px
import warnings
from datetime import datetime, timedelta, date
import calendar
import json
import os
from io import StringIO

# Отключаем предупреждение о кэше
warnings.filterwarnings('ignore', message='file_cache is unavailable when using oauth2client >= 4.0.0')

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# Константы
NUMERIC_COLUMNS = ['руки', 'RB%_PLAYER', 'Rake_USD_PLAYER', 'RB_USD_PLAYER', 'Win_USD_PLAYER', 'Profit_PLAYER']
SPREADSHEET_ID = '1vtB1IFryFOiM13EfPFD60kQ69KaYxLQtrtB4KOsoZKo'
CACHE_PATH = '.streamlit/cache/'
CACHE_FILE = 'data_cache.json'

def get_date_ranges() -> Dict[str, Tuple[date, date]]:
    """
    Возвращает диапазоны дат для фильтрации
    
    Returns:
        Dict[str, Tuple[date, date]]: Словарь с диапазонами дат
    """
    today = date.today()
    first_day_current = today.replace(day=1)
    thirty_days_ago = today - timedelta(days=30)
    
    return {
        'current_month': (first_day_current, today),
        'last_30_days': (thirty_days_ago, today)
    }

class DataCache:
    def __init__(self):
        self.cache_path = CACHE_PATH
        self.cache_file = CACHE_FILE
        os.makedirs(self.cache_path, exist_ok=True)
    
    def save_to_cache(self, data: pd.DataFrame):
        """
        Сохраняет данные в кэш
        
        Args:
            data (pd.DataFrame): DataFrame для сохранения
        """
        try:
            data_copy = data.copy()
            date_column = data_copy.columns[0]
            
            # Преобразуем даты в строки перед сохранением
            data_copy[date_column] = data_copy[date_column].apply(
                lambda x: x.isoformat() if isinstance(x, date) else None
            )
            
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': data_copy.to_json(date_format='iso', orient='split')
            }
            
            with open(os.path.join(self.cache_path, self.cache_file), 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False)
                
            logger.info("Данные успешно сохранены в кэш")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в кэш: {e}")
            raise
    
    def load_from_cache(self) -> Optional[pd.DataFrame]:
        """
        Загружает данные из кэша
        
        Returns:
            Optional[pd.DataFrame]: Загруженный DataFrame или None в случае ошибки
        """
        try:
            cache_file = os.path.join(self.cache_path, self.cache_file)
            if not os.path.exists(cache_file):
                logger.info("Кэш-файл не найден")
                return None
            
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Проверяем актуальность кэша (24 часа)
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp'])
            if datetime.now() - cache_timestamp > timedelta(days=1):
                logger.info("Кэш устарел")
                return None
            
            # Используем StringIO для чтения JSON
            df = pd.read_json(StringIO(cache_data['data']), orient='split')
            
            # Преобразуем даты обратно в объекты date
            date_column = df.columns[0]
            df[date_column] = pd.to_datetime(df[date_column]).dt.date
            
            logger.info("Данные успешно загружены из кэша")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке из кэша: {e}")
            return None
    
    def clear_cache(self):
        """Очищает кэш"""
        try:
            cache_file = os.path.join(self.cache_path, self.cache_file)
            if os.path.exists(cache_file):
                os.remove(cache_file)
                logger.info("Кэш успешно очищен")
        except Exception as e:
            logger.error(f"Ошибка при очистке кэша: {e}")
            raise

    def get_cache_info(self) -> Dict[str, Any]:
        """
        Получает информацию о состоянии кэша
        
        Returns:
            Dict[str, Any]: Информация о кэше
        """
        try:
            cache_file = os.path.join(self.cache_path, self.cache_file)
            if not os.path.exists(cache_file):
                return {
                    'exists': False,
                    'size': 0,
                    'last_modified': None,
                    'age_hours': None
                }
            
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            timestamp = datetime.fromisoformat(cache_data['timestamp'])
            age = datetime.now() - timestamp
            
            return {
                'exists': True,
                'size': os.path.getsize(cache_file),
                'last_modified': timestamp,
                'age_hours': age.total_seconds() / 3600
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о кэше: {e}")
            return {
                'exists': False,
                'size': 0,
                'last_modified': None,
                'age_hours': None,
                'error': str(e)
            }

class DataLoader:
    def __init__(self):
        """
        Инициализация загрузчика данных.
        Устанавливает соединение с Google Sheets и создает объект кеша.
        """
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
            self.cache = DataCache()
        except Exception as e:
            logger.error(f"Ошибка при инициализации DataLoader: {e}")
            st.error("Ошибка при инициализации подключения к Google Sheets")

    def _format_date(self, date_str: str) -> date:
        """
        Преобразует строку даты в объект date.
        
        Args:
            date_str: Строка с датой или объект date
            
        Returns:
            date: Объект datetime.date или None в случае ошибки
        """
        if isinstance(date_str, date):
            return date_str
            
        if pd.isna(date_str):
            return None
            
        try:
            # Пробуем формат DD.MM.YYYY
            return datetime.strptime(str(date_str).strip(), '%d.%m.%Y').date()
        except ValueError:
            try:
                # Пробуем другие форматы через pandas
                return pd.to_datetime(date_str).date()
            except:
                logger.error(f"Не удалось преобразовать дату: {date_str}")
                return None

    def _process_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрабатывает числовые столбцы DataFrame.
        
        Args:
            df: Исходный DataFrame
            
        Returns:
            DataFrame: Обработанный DataFrame
        """
        try:
            df = df.copy()
            
            for col in NUMERIC_COLUMNS:
                if col in df.columns:
                    df[col] = (df[col]
                              .replace('', np.nan)
                              .replace('-', np.nan)
                              .str.replace('\xa0', '', regex=False)
                              .str.replace(',', '.', regex=False)
                              .str.replace(' ', '', regex=False)
                              .pipe(pd.to_numeric, errors='coerce'))
            
            # Фильтруем строки, где тренер = '0'
            df = df[df['Тренер'] != '0']
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при обработке числовых столбцов: {e}")
            st.error("Ошибка при обработке данных")
            return df

    def _fetch_from_sheets(self) -> Optional[pd.DataFrame]:
        """
        Загружает данные из Google Sheets.
        
        Returns:
            Optional[pd.DataFrame]: Загруженный DataFrame или None в случае ошибки
        """
        try:
            logger.info("Загрузка данных из Google Sheets")
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range="pivot_result"
            ).execute()
            values = result.get("values", [])
            
            if not values:
                st.error("Данные не найдены в Google Sheets")
                return None
            
            df = pd.DataFrame(values[1:], columns=values[0])
            
            date_column = df.columns[0]
            df[date_column] = df[date_column].apply(self._format_date)
            
            # Удаляем строки с некорректными датами
            df = df.dropna(subset=[date_column])
            
            return self._process_numeric_columns(df)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из Google Sheets: {e}")
            st.error(f"Ошибка при загрузке данных: {str(e)}")
            return None

    def _validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Проверяет корректность загруженного DataFrame.
        
        Args:
            df: DataFrame для проверки
            
        Returns:
            bool: True если DataFrame корректен, False в противном случае
        """
        if df is None or df.empty:
            return False
            
        required_columns = ['Тренер', 'nickname', 'club_name'] + NUMERIC_COLUMNS
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Отсутствует обязательный столбец: {col}")
                return False
                
        date_column = df.columns[0]
        if df[date_column].isna().all():
            logger.error("Все значения в столбце даты отсутствуют")
            return False
            
        return True

    def load_data(self, force_reload: bool = False) -> Optional[pd.DataFrame]:
        """
        Загружает данные из кеша или Google Sheets.
        
        Args:
            force_reload: Принудительная загрузка из Google Sheets
            
        Returns:
            Optional[pd.DataFrame]: Загруженный DataFrame или None в случае ошибки
        """
        try:
            df = None
            
            # Пытаемся загрузить из кеша, если не требуется принудительное обновление
            if not force_reload:
                df = self.cache.load_from_cache()
                if df is not None:
                    logger.info("Данные успешно загружены из кеша")
                    
                    # Проверяем и преобразуем даты после загрузки из кеша
                    date_column = df.columns[0]
                    df[date_column] = df[date_column].apply(self._format_date)
                    df = df.dropna(subset=[date_column])
                    
                    if self._validate_dataframe(df):
                        return df
                    else:
                        logger.warning("Данные в кеше повреждены")
            
            # Если нет данных в кеше или требуется обновление, загружаем из Google Sheets
            df = self._fetch_from_sheets()
            
            if df is not None and self._validate_dataframe(df):
                # Сохраняем успешно загруженные данные в кеш
                self.cache.save_to_cache(df)
                logger.info("Данные успешно сохранены в кеш")
                return df
            else:
                logger.error("Не удалось загрузить корректные данные")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            st.error(f"Ошибка при загрузке данных: {str(e)}")
            return None

    def clear_cache(self) -> bool:
        """
        Очищает кеш данных.
        
        Returns:
            bool: True если очистка успешна, False в противном случае
        """
        try:
            self.cache.clear_cache()
            logger.info("Кеш успешно очищен")
            return True
        except Exception as e:
            logger.error(f"Ошибка при очистке кеша: {e}")
            return False
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
            self.cache = DataCache()
        except Exception as e:
            logger.error(f"Ошибка при инициализации DataLoader: {e}")
            st.error("Ошибка при инициализации подключения к Google Sheets")

    def _format_date(self, date_str: str) -> str:
        """Преобразует строку даты в правильный формат"""
        try:
            return pd.to_datetime(date_str, format='%d.%m.%Y').date()
        except:
            try:
                return pd.to_datetime(date_str).date()
            except:
                logger.error(f"Не удалось преобразовать дату: {date_str}")
                return None

    def load_data(self, force_reload: bool = False) -> Optional[pd.DataFrame]:
        """
        Загружает данные из кэша или Google Sheets
        
        Args:
            force_reload (bool): Принудительная загрузка из Google Sheets
        """
        try:
            if not force_reload:
                cached_data = self.cache.load_from_cache()
                if cached_data is not None:
                    logger.info("Данные загружены из кэша")
                    return cached_data
            
            logger.info("Загрузка данных из Google Sheets")
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range="pivot_result"
            ).execute()
            values = result.get("values", [])
            
            if not values:
                st.error("Данные не найдены")
                return None
            
            df = pd.DataFrame(values[1:], columns=values[0])
            date_column = df.columns[0]
            df[date_column] = df[date_column].apply(self._format_date)
            
            processed_df = self._process_numeric_columns(df)
            if processed_df is not None:
                self.cache.save_to_cache(processed_df)
                logger.info("Данные сохранены в кэш")
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            st.error(f"Ошибка при загрузке данных: {str(e)}")
            return None

    def _process_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обрабатывает числовые столбцы DataFrame"""
        try:
            df = df.copy()
            for col in NUMERIC_COLUMNS:
                if col in df.columns:
                    df[col] = (df[col]
                              .replace('', np.nan)
                              .str.replace('\xa0', '')
                              .str.replace(',', '.')
                              .pipe(pd.to_numeric, errors='coerce'))
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
            
            # Убеждаемся, что даты в DataFrame правильного типа
            date_column = self.data.columns[0]
            self.data[date_column] = pd.to_datetime(self.data[date_column]).dt.date
            
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
            
            # Приводим даты к нужному типу для фильтрации
            period_filtered_data = self.data[
                (self.data[date_column] >= pd.to_datetime(start_date).date()) &
                (self.data[date_column] <= pd.to_datetime(end_date).date())
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
                "grouping": ['Тренер', 'nickname'],
                "start_date": pd.to_datetime(start_date).date(),
                "end_date": pd.to_datetime(end_date).date()
            }

    def apply_filters(self, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Применяет фильтры к данным
        """
        filtered_data = self.data.copy()
        date_column = filtered_data.columns[0]
        
        # Убеждаемся, что даты в правильном формате
        filtered_data[date_column] = pd.to_datetime(filtered_data[date_column]).dt.date
        
        # Применяем фильтры
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
        """Обрабатывает и отображает данные"""
        agg_dict = {
            'Profit_PLAYER': 'sum',
            'Win_USD_PLAYER': 'sum',
            'RB_USD_PLAYER': 'sum',
            'руки': 'sum',
            'Rake_USD_PLAYER': 'sum'
        }
        
        grouped_data = (filtered_data
                       .groupby(params["grouping"], as_index=False)
                       .agg(agg_dict)
                       .round(0)
                       .sort_values(by='Profit_PLAYER', ascending=False))
        
        grouped_data.insert(0, "Выбрать", False)
        
        col1, col2 = st.columns([4, 4])
        
        with col1:
            st.subheader("Сгруппированные данные")
            
            config = {
                "Выбрать": st.column_config.CheckboxColumn(
                    "Выбрать",
                    help="Выберите строки для фильтрации",
                    default=False,
                )
            }
            
            edited_df = st.data_editor(
                grouped_data,
                hide_index=True,
                column_config=config,
                disabled=grouped_data.columns.difference(["Выбрать"]).tolist(),
                key="data_editor",
                height=600,
                use_container_width=True
            )
            
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
            
            filtered_grouped_data = filtered_grouped_data.drop(columns=["Выбрать"])
            
            st.subheader("Статистика по клубам")
            
            club_data = (filtered_for_visualization
                        .groupby('club_name', as_index=False)
                        .agg(agg_dict)
                        .round(0)
                        .sort_values(by='Profit_PLAYER', ascending=False))
            

                
                
            club_data = club_data.rename(columns={
                'club_name': 'Клуб',
                'Profit_PLAYER': 'Профит',
                'Win_USD_PLAYER': 'Выигрыш',
                'RB_USD_PLAYER': 'РБ',
                'руки': 'Руки',
                'Rake_USD_PLAYER': 'Рейк'
            })
            
            totals = club_data.sum(numeric_only=True).round(0)
            totals = pd.DataFrame([totals], columns=club_data.columns[1:])
            totals.insert(0, 'Клуб', 'ИТОГО')
            club_data = pd.concat([club_data, totals], ignore_index=True)
            
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
        
        with col2:
            metric_cols = st.columns(5)
            
            total_profit = float(filtered_grouped_data['Profit_PLAYER'].sum())
            total_win = float(filtered_grouped_data['Win_USD_PLAYER'].sum())
            total_rb = float(filtered_grouped_data['RB_USD_PLAYER'].sum())
            total_hands = float(filtered_grouped_data['руки'].sum())
            total_rake = float(filtered_grouped_data['Rake_USD_PLAYER'].sum())
            
            metric_cols[0].metric("Profit", f"${total_profit:,.0f}")
            metric_cols[1].metric("Win", f"${total_win:,.0f}")
            metric_cols[2].metric("RB", f"${total_rb:,.0f}")
            metric_cols[3].metric("Руки", f"{total_hands:,.0f}")
            metric_cols[4].metric("Rake", f"${total_rake:,.0f}")
            
            self.display_visualizations(filtered_for_visualization, params)

    def display_visualizations(self, filtered_data: pd.DataFrame, params: Dict[str, Any]):
        """Отображает визуализации данных"""
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

    def load_data(self):
        """Загружает данные с возможностью обновления"""
        col1, col2 = st.columns([6, 1])
        with col2:
            if st.button("🔄 Обновить данные", key="refresh_data"):
                with st.spinner("Обновление данных..."):
                    self.data = self.data_loader.load_data(force_reload=True)
                    st.success("Данные обновлены!")
                    st.experimental_rerun()
        
        if self.data is None:
            with st.spinner("Загрузка данных..."):
                self.data = self.data_loader.load_data()

    def run(self):
        """Запускает приложение"""
        st.title("Анализ данных игроков")
        
        # Загрузка данных с возможностью обновления
        self.load_data()
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
