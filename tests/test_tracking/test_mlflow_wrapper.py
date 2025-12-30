# tests/test_tracking/test_mlflow_wrapper.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import os
import mlflow
from yavai.tracking.mlflow_wrapper import MLflowWrapper


@pytest.fixture
def wrapper():
    return MLflowWrapper()


@patch('yavai.config.AWS_ACCESS_KEY_ID', 'test_key')
@patch('yavai.config.AWS_SECRET_ACCESS_KEY', 'test_secret')
@patch('yavai.config.MLFLOW_S3_ENDPOINT_URL', 'http://minio:9000')
def test_configure_aws_credentials(wrapper):
    wrapper.configure_aws_credentials()
    
    assert os.environ['AWS_ACCESS_KEY_ID'] == 'test_key'
    assert os.environ['AWS_SECRET_ACCESS_KEY'] == 'test_secret'
    assert os.environ['MLFLOW_S3_ENDPOINT_URL'] == 'http://minio:9000'


@patch('yavai.config.AWS_ACCESS_KEY_ID', None)
def test_configure_aws_credentials_incomplete(wrapper):
    wrapper.configure_aws_credentials()
    # Should not raise, just warn


@patch('mlflow.start_run')
def test_start_run(mock_start, wrapper):
    mock_run = Mock()
    mock_start.return_value = mock_run
    
    result = wrapper.start_run(run_name='test_run')
    
    assert result == mock_run
    mock_start.assert_called_once_with(run_name='test_run')


@patch('mlflow.end_run')
def test_end_run(mock_end, wrapper):
    wrapper.end_run(status='FINISHED')
    
    mock_end.assert_called_once_with('FINISHED')


@patch('mlflow.active_run')
def test_active_run(mock_active, wrapper):
    mock_run = Mock()
    mock_active.return_value = mock_run
    
    result = wrapper.active_run()
    
    assert result == mock_run


@patch('mlflow.log_param')
def test_log_param(mock_log, wrapper):
    wrapper.log_param('learning_rate', 0.001)
    
    mock_log.assert_called_once_with('learning_rate', 0.001)


@patch('mlflow.log_params')
def test_log_params(mock_log, wrapper):
    params = {'lr': 0.001, 'batch_size': 32}
    wrapper.log_params(params)
    
    mock_log.assert_called_once_with(params)


@patch('mlflow.log_metric')
def test_log_metric(mock_log, wrapper):
    wrapper.log_metric('loss', 0.5, step=10)
    
    mock_log.assert_called_once_with('loss', 0.5, step=10)


@patch('mlflow.log_metrics')
def test_log_metrics(mock_log, wrapper):
    metrics = {'loss': 0.5, 'accuracy': 0.9}
    wrapper.log_metrics(metrics, step=10)
    
    mock_log.assert_called_once_with(metrics, step=10)


@patch('mlflow.set_tag')
def test_set_tag(mock_set, wrapper):
    wrapper.set_tag('model_type', 'cnn')
    
    mock_set.assert_called_once_with('model_type', 'cnn')


@patch('mlflow.log_text')
def test_log_text(mock_log, wrapper):
    wrapper.log_text('sample text', 'notes.txt')
    
    mock_log.assert_called_once_with('sample text', 'notes.txt')


@patch('mlflow.log_dict')
def test_log_dict(mock_log, wrapper):
    data = {'key': 'value'}
    wrapper.log_dict(data, 'config.json')
    
    mock_log.assert_called_once_with(data, 'config.json')


@patch('mlflow.set_tracking_uri')
@patch('mlflow.autolog')
@patch('getpass.getuser', return_value='testuser')
def test_set_tracking_uri_direct(mock_user, mock_autolog, mock_set_uri, wrapper):
    wrapper.set_tracking_uri('http://mlflow:5000')
    
    mock_set_uri.assert_called_once_with('http://mlflow:5000')
    mock_autolog.assert_called_once()


@patch('mlflow.set_tracking_uri')
@patch('mlflow.autolog')
@patch('getpass.getuser', return_value='jovyan')
def test_set_tracking_uri_jovyan_warning(mock_user, mock_autolog, mock_set_uri, wrapper):
    wrapper.set_tracking_uri('yavai')
    
    mock_set_uri.assert_called_once_with(None)


@patch('requests.get')
@patch('mlflow.set_tracking_uri')
@patch('mlflow.autolog')
@patch('getpass.getuser', return_value='testuser')
@patch('yavai.config.API_URL2', 'http://api/mlflow-uri?username=')
def test_set_tracking_uri_from_api(mock_user, mock_autolog, mock_set_uri, mock_get, wrapper):
    mock_response = Mock()
    mock_response.json.return_value = {
        'status': 200,
        'url_mlflow': 'http://mlflow:5000'
    }
    mock_get.return_value = mock_response
    
    wrapper.set_tracking_uri('yavai')
    
    mock_set_uri.assert_called_once_with('http://mlflow:5000')


@patch('mlflow.set_experiment')
@patch('mlflow.get_tracking_uri', return_value='http://mlflow:5000')
@patch('getpass.getuser', return_value='testuser')
def test_set_experiment(mock_user, mock_get_uri, mock_set_exp, wrapper):
    with patch.object(wrapper, '_update_experiment_status'):
        wrapper.set_experiment('test_experiment')
        
        mock_set_exp.assert_called_once_with('test_experiment')


@patch('mlflow.get_tracking_uri', return_value='file:///tmp/mlruns')
def test_set_experiment_invalid_uri(mock_get_uri, wrapper):
    wrapper.set_experiment('test_experiment')
    # Should log error, not raise


@patch('mlflow.sklearn.log_model')
def test_log_model_sklearn(mock_log, wrapper):
    model = Mock()
    model.__class__.__module__ = 'sklearn.linear_model'
    
    wrapper.log_model(model, 'model')
    
    mock_log.assert_called_once()


@patch('mlflow.pyfunc.log_model')
def test_log_model_generic(mock_log, wrapper):
    model = Mock()
    
    wrapper.log_model(model, 'model')
    
    mock_log.assert_called_once()