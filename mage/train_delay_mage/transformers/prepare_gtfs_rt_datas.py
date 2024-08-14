if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def dispatch_feed(feed_dict:dict, *args, **kwargs) -> dict:
    """

    """
    # Remove Header key
    del feed_dict['header']
    # Remove entity key and keep the list value as dictionary
    feed_dict = feed_dict['entity']


    return feed_dict



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
