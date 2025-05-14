# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from typing import List, Dict, Set
from deadline.client.cli._groups.click_logger import ClickLogger


class SessionActionProcessor:
    def __init__(
        self,
        auxiliary_session_action_status_mapping: Dict[str, str],
        logger: ClickLogger,
        ongoing_sessions: Set[str],
        session_action_index_map: Dict[str, int],
        session_to_last_finished_action_id_map: Dict[str, int],
    ):
        self.auxiliary_session_action_status_mapping = auxiliary_session_action_status_mapping
        self.logger = logger
        self.ongoing_sessions = ongoing_sessions
        self.session_action_index_map = session_action_index_map
        self.session_to_last_finished_action_id_map = session_to_last_finished_action_id_map
        self.new_session_actions: List[Dict[str, str]] = []
        self.downloaded_action_ids: List[int] = []

    def hydrate_and_process_session_actions(self) -> None:
        """
        Hydrate and process ongoing and finished session actions
        """
        # Process each ongoing session
        for session_id in self.ongoing_sessions:
            # Get the last downloaded session action ID for this session
            last_downloaded_action_id = self.session_action_index_map.get(session_id, -1)

            # TODO: Implement the actual API call for listing session actions for this session
            # For now, we'll simulate with an empty list
            session_actions: List[Dict] = []

            # Filter for session actions that are newer than the last downloaded one
            new_session_actions = [
                action
                for action in session_actions
                if self._get_session_action_index_from_action(str(action.get("sessionActionId")))
                > last_downloaded_action_id
            ]

            # Add the highest index session action to the session_to_last_finished_action_id_map
            if new_session_actions:
                highest_index = max(
                    self._get_session_action_index_from_action(str(action.get("sessionActionId")))
                    for action in new_session_actions
                )
                self.session_to_last_finished_action_id_map[session_id] = highest_index
                self.new_session_actions = new_session_actions

            if not new_session_actions:
                self.logger.echo(f"No new session actions found for session {session_id}")
                continue

            # Download outputs from the completed session actions
            self.process_and_download_session_action_outputs()

            #  Update the last downloaded session action ID for this session
            if self.downloaded_action_ids:
                self.session_action_index_map[session_id] = max(self.downloaded_action_ids)

    def process_and_download_session_action_outputs(self) -> None:
        """
        Process and download outputs from completed session actions.
        """
        # No need to initialize lists as they are already initialized in __init__

        for action in self.new_session_actions:
            action_id: str = str(action.get("sessionActionId"))
            action_status: str = str(action.get("sessionActionStatus"))
            self.auxiliary_session_action_status_mapping[action_id] = action_status

            """
            Terminal statuses for session actions are:
            SUCCEEDED
            FAILED
            INTERRUPTED
            CANCELED
            """
            if action_status in ["SUCCEEDED", "FAILED", "INTERRUPTED", "CANCELED"]:
                try:
                    # a. Create merged manifest
                    self.logger.echo(f"Creating merged manifest for session action {action_id}")
                    # TODO: Implement merged manifest creation

                    # b. Download outputs from merged manifest
                    self.logger.echo(f"Downloading outputs for session action {action_id}")
                    # TODO: Implement output download using the merged manifest
                    download_success = True  # This would be the result of the download operation

                    if download_success:
                        # c. Add to list of downloaded session action ids
                        action_id_number: int = self._get_session_action_index_from_action(
                            action_id
                        )
                        self.downloaded_action_ids.append(action_id_number)
                        self.logger.echo(
                            f"Successfully downloaded outputs for session action {action_id}"
                        )
                    else:
                        self.logger.echo(
                            f"Failed to download outputs for session action {action_id}"
                        )

                except Exception as e:
                    self.logger.echo(f"Error processing session action {action_id}: {str(e)}")

    def _get_session_action_index_from_action(self, action_id: str) -> int:
        """
        Get the index of the session action from the action id string
        :param action_id: Session action id, eg. Session-1234334-1
        :return: action_id index at the end of the session action
        """
        # Get action id number from string Session-123454645-1, the last part is the number
        action_id_number: int = int(action_id.split("-")[2])
        return action_id_number
